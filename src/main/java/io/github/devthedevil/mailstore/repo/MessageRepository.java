package io.github.devthedevil.mailstore.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import io.github.devthedevil.mailstore.model.Flag;
import io.github.devthedevil.mailstore.model.Folder;
import io.github.devthedevil.mailstore.model.Message;
import io.github.devthedevil.mailstore.model.MessageSummary;
import io.github.devthedevil.mailstore.util.TimeBuckets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

/**
 * Fully asynchronous data access for messages. Every public method returns a
 * {@link CompletionStage} and never blocks a calling thread: all composition
 * happens on the driver's internal Netty event loop via {@code executeAsync}.
 */
public final class MessageRepository {

    /** How many month buckets a folder listing walks back before giving up. */
    private static final int MAX_BUCKETS_PER_LISTING = 12;
    /** Retries for the optimistic (LWT) flag update under concurrent writers. */
    private static final int MAX_LWT_RETRIES = 3;
    /** Messages in TRASH auto-expire via Cassandra TTL — no purge job needed. */
    public static final int TRASH_TTL_SECONDS = (int) Duration.ofDays(30).toSeconds();

    private final CqlSession session;

    private final PreparedStatement insertById;
    private final PreparedStatement insertListing;
    private final PreparedStatement insertListingWithTtl;
    private final PreparedStatement selectById;
    private final PreparedStatement selectListing;
    private final PreparedStatement selectFlagsById;
    private final PreparedStatement updateFlagsByIdIfUnchanged;
    private final PreparedStatement addFlagsToListing;
    private final PreparedStatement deleteListing;
    private final PreparedStatement updateFolderById;

    public MessageRepository(CqlSession session) {
        this.session = session;
        this.insertById = session.prepare(
                "INSERT INTO mailstore.messages_by_id "
                        + "(user_id, message_id, folder, sender, recipients, subject, body, size_bytes, flags) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        this.insertListing = session.prepare(
                "INSERT INTO mailstore.messages_by_mailbox "
                        + "(user_id, folder, bucket, message_id, sender, subject, preview, size_bytes, flags) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        this.insertListingWithTtl = session.prepare(
                "INSERT INTO mailstore.messages_by_mailbox "
                        + "(user_id, folder, bucket, message_id, sender, subject, preview, size_bytes, flags) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?");
        this.selectById = session.prepare(
                "SELECT user_id, message_id, folder, sender, recipients, subject, body, size_bytes, flags "
                        + "FROM mailstore.messages_by_id WHERE user_id = ? AND message_id = ?");
        this.selectListing = session.prepare(
                "SELECT user_id, folder, message_id, sender, subject, preview, size_bytes, flags "
                        + "FROM mailstore.messages_by_mailbox WHERE user_id = ? AND folder = ? AND bucket = ?");
        this.selectFlagsById = session.prepare(
                "SELECT flags FROM mailstore.messages_by_id WHERE user_id = ? AND message_id = ?");
        this.updateFlagsByIdIfUnchanged = session.prepare(
                "UPDATE mailstore.messages_by_id SET flags = ? "
                        + "WHERE user_id = ? AND message_id = ? IF flags = ?");
        this.addFlagsToListing = session.prepare(
                "UPDATE mailstore.messages_by_mailbox SET flags = flags + ? "
                        + "WHERE user_id = ? AND folder = ? AND bucket = ? AND message_id = ?");
        this.deleteListing = session.prepare(
                "DELETE FROM mailstore.messages_by_mailbox "
                        + "WHERE user_id = ? AND folder = ? AND bucket = ? AND message_id = ?");
        this.updateFolderById = session.prepare(
                "UPDATE mailstore.messages_by_id SET folder = ? WHERE user_id = ? AND message_id = ?");
    }

    /**
     * Persists a message into both query tables in a single LOGGED batch.
     * The batchlog guarantees both denormalized views eventually agree even if
     * a node fails mid-write — the standard trade-off (extra write latency for
     * multi-view consistency) when maintaining denormalized tables by hand.
     */
    public CompletionStage<Void> save(Message message) {
        int bucket = TimeBuckets.bucketOf(message.messageId());
        BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                .addStatement(insertById.bind(
                        message.userId(), message.messageId(), message.folder().name(),
                        message.sender(), message.recipients(), message.subject(),
                        message.body(), message.sizeBytes(), toStrings(message.flags())))
                .addStatement(insertListing.bind(
                        message.userId(), message.folder().name(), bucket, message.messageId(),
                        message.sender(), message.subject(), message.preview(),
                        message.sizeBytes(), toStrings(message.flags())))
                .build();
        return session.executeAsync(batch).thenApply(rs -> null);
    }

    public CompletionStage<Optional<Message>> findById(String userId, UUID messageId) {
        return session.executeAsync(selectById.bind(userId, messageId))
                .thenApply(rs -> Optional.ofNullable(rs.one()).map(MessageRepository::toMessage));
    }

    /**
     * Lists the newest {@code limit} messages of a folder, newest first.
     * Starts at the current month bucket and walks backwards, composing one
     * async query per bucket until the limit is met — no thread ever blocks
     * between bucket hops.
     */
    public CompletionStage<List<MessageSummary>> fetchRecent(String userId, Folder folder, int limit) {
        int currentBucket = TimeBuckets.bucketOf(java.time.Instant.now());
        return fetchFromBucket(userId, folder, currentBucket, limit,
                new ArrayList<>(), MAX_BUCKETS_PER_LISTING);
    }

    private CompletionStage<List<MessageSummary>> fetchFromBucket(
            String userId, Folder folder, int bucket, int limit,
            List<MessageSummary> collected, int bucketsLeft) {
        if (collected.size() >= limit || bucketsLeft == 0) {
            return completedFuture(collected);
        }
        BoundStatement stmt = selectListing.bind(userId, folder.name(), bucket)
                .setPageSize(limit - collected.size());
        return session.executeAsync(stmt)
                .thenCompose(rs -> drainPages(rs, limit, collected))
                .thenCompose(acc -> fetchFromBucket(
                        userId, folder, TimeBuckets.previous(bucket), limit, acc, bucketsLeft - 1));
    }

    /** Accumulates rows across the driver's async result pages until {@code limit}. */
    private CompletionStage<List<MessageSummary>> drainPages(
            AsyncResultSet rs, int limit, List<MessageSummary> collected) {
        for (Row row : rs.currentPage()) {
            if (collected.size() >= limit) {
                return completedFuture(collected);
            }
            collected.add(toSummary(row));
        }
        if (collected.size() < limit && rs.hasMorePages()) {
            return rs.fetchNextPage().thenCompose(next -> drainPages(next, limit, collected));
        }
        return completedFuture(collected);
    }

    /**
     * Marks a message read using an optimistic read-modify-write: the flag set
     * is rewritten with a lightweight transaction ({@code IF flags = <seen>}),
     * so two devices racing to update flags can never lose each other's write.
     * Retries up to {@link #MAX_LWT_RETRIES} times on contention.
     *
     * @return {@code true} if this call transitioned the message to read,
     *         {@code false} if it was already read or does not exist.
     */
    public CompletionStage<Boolean> markRead(String userId, Folder folder, UUID messageId) {
        return markRead(userId, folder, messageId, MAX_LWT_RETRIES);
    }

    private CompletionStage<Boolean> markRead(
            String userId, Folder folder, UUID messageId, int retriesLeft) {
        if (retriesLeft == 0) {
            return failedFuture(new IllegalStateException(
                    "markRead gave up after " + MAX_LWT_RETRIES + " contended LWT attempts"));
        }
        return session.executeAsync(selectFlagsById.bind(userId, messageId))
                .thenCompose(rs -> {
                    Row row = rs.one();
                    if (row == null) {
                        return completedFuture(false);
                    }
                    Set<String> seen = row.getSet("flags", String.class);
                    if (seen.contains(Flag.READ.name())) {
                        return completedFuture(false);
                    }
                    Set<String> updated = new HashSet<>(seen);
                    updated.add(Flag.READ.name());
                    // Cassandra stores an empty set as null, so the LWT
                    // condition must compare against null in that case.
                    Set<String> condition = seen.isEmpty() ? null : seen;
                    return session.executeAsync(
                                    updateFlagsByIdIfUnchanged.bind(updated, userId, messageId, condition))
                            .thenCompose(lwt -> lwt.wasApplied()
                                    ? propagateReadFlagToListing(userId, folder, messageId)
                                    : markRead(userId, folder, messageId, retriesLeft - 1));
                });
    }

    private CompletionStage<Boolean> propagateReadFlagToListing(
            String userId, Folder folder, UUID messageId) {
        int bucket = TimeBuckets.bucketOf(messageId);
        return session.executeAsync(addFlagsToListing.bind(
                        Set.of(Flag.READ.name()), userId, folder.name(), bucket, messageId))
                .thenApply(rs -> true);
    }

    /**
     * Moves a message to TRASH: deletes the old listing row, writes the trash
     * listing row with a 30-day TTL (Cassandra expires it — no cron purge),
     * and repoints the canonical row, all in one LOGGED batch.
     *
     * @return the message as it was before the move, or empty if not found.
     */
    public CompletionStage<Optional<Message>> moveToTrash(String userId, Folder folder, UUID messageId) {
        return findById(userId, messageId).thenCompose(found -> {
            if (found.isEmpty()) {
                return completedFuture(Optional.empty());
            }
            Message m = found.get();
            int bucket = TimeBuckets.bucketOf(messageId);
            BatchStatement batch = BatchStatement.builder(DefaultBatchType.LOGGED)
                    .addStatement(deleteListing.bind(userId, folder.name(), bucket, messageId))
                    .addStatement(insertListingWithTtl.bind(
                            userId, Folder.TRASH.name(), bucket, messageId,
                            m.sender(), m.subject(), m.preview(), m.sizeBytes(),
                            toStrings(m.flags()), TRASH_TTL_SECONDS))
                    .addStatement(updateFolderById.bind(Folder.TRASH.name(), userId, messageId))
                    .build();
            return session.executeAsync(batch).thenApply(rs -> Optional.of(m));
        });
    }

    private static Set<String> toStrings(Set<Flag> flags) {
        return flags.stream().map(Enum::name).collect(Collectors.toSet());
    }

    private static Set<Flag> toFlags(Set<String> names) {
        return names.stream().map(Flag::valueOf).collect(Collectors.toSet());
    }

    private static Message toMessage(Row row) {
        return new Message(
                row.getString("user_id"),
                row.getUuid("message_id"),
                Folder.valueOf(row.getString("folder")),
                row.getString("sender"),
                row.getList("recipients", String.class),
                row.getString("subject"),
                row.getString("body"),
                row.getInt("size_bytes"),
                toFlags(row.getSet("flags", String.class)));
    }

    private static MessageSummary toSummary(Row row) {
        return new MessageSummary(
                row.getString("user_id"),
                row.getUuid("message_id"),
                Folder.valueOf(row.getString("folder")),
                row.getString("sender"),
                row.getString("subject"),
                row.getString("preview"),
                row.getInt("size_bytes"),
                toFlags(row.getSet("flags", String.class)));
    }
}
