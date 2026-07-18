package io.github.devthedevil.mailstore;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.github.devthedevil.mailstore.config.CqlSessionFactory;
import io.github.devthedevil.mailstore.model.Flag;
import io.github.devthedevil.mailstore.model.Folder;
import io.github.devthedevil.mailstore.model.MailboxStats;
import io.github.devthedevil.mailstore.model.Message;
import io.github.devthedevil.mailstore.model.MessageSummary;
import io.github.devthedevil.mailstore.repo.MessageRepository;
import io.github.devthedevil.mailstore.schema.SchemaInitializer;
import io.github.devthedevil.mailstore.util.TimeBuckets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests against a real Cassandra node (Testcontainers).
 * Each test uses its own user id, so tests are isolated despite sharing
 * one container and keyspace.
 */
@Testcontainers
class MailServiceIT {

    @Container
    private static final CassandraContainer<?> CASSANDRA =
            new CassandraContainer<>("cassandra:5.0")
                    .withEnv("MAX_HEAP_SIZE", "1024M")
                    .withEnv("HEAP_NEWSIZE", "128M")
                    .withStartupTimeout(Duration.ofMinutes(5));

    private static CqlSession session;
    private static MailService mail;

    @BeforeAll
    static void setUp() {
        session = CqlSessionFactory.create(
                CASSANDRA.getContactPoint(), CASSANDRA.getLocalDatacenter());
        SchemaInitializer.apply(session);
        mail = new MailService(session);
    }

    @AfterAll
    static void tearDown() {
        if (session != null) {
            session.close();
        }
    }

    @Test
    void deliversAndListsNewestFirstWithAccurateCounters() {
        String user = "list-user@example.com";
        Message oldest = newMessage(user, "a@example.com", "first");
        Message middle = newMessage(user, "b@example.com", "second");
        Message newest = newMessage(user, "c@example.com", "third");
        for (Message m : List.of(oldest, middle, newest)) {
            join(mail.deliver(m));
        }

        List<MessageSummary> inbox = join(mail.listFolder(user, Folder.INBOX, 10));

        assertThat(inbox).extracting(MessageSummary::subject)
                .containsExactly("third", "second", "first");
        MailboxStats stats = join(mail.getStats(user, Folder.INBOX));
        assertThat(stats.totalMessages()).isEqualTo(3);
        assertThat(stats.unreadMessages()).isEqualTo(3);
    }

    @Test
    void opensFullMessageBodyFromCanonicalTable() {
        String user = "open-user@example.com";
        Message sent = newMessage(user, "sender@example.com", "hello");
        join(mail.deliver(sent));

        Optional<Message> opened = join(mail.open(user, sent.messageId()));

        assertThat(opened).isPresent();
        assertThat(opened.get().body()).isEqualTo("body of hello");
        assertThat(opened.get().recipients()).containsExactly(user);
    }

    @Test
    void markReadTransitionsOnceAndDecrementsUnreadExactlyOnce() {
        String user = "read-user@example.com";
        Message m = newMessage(user, "sender@example.com", "to read");
        join(mail.deliver(m));

        assertThat(join(mail.markRead(user, Folder.INBOX, m.messageId()))).isTrue();
        assertThat(join(mail.markRead(user, Folder.INBOX, m.messageId()))).isFalse();

        MailboxStats stats = join(mail.getStats(user, Folder.INBOX));
        assertThat(stats.unreadMessages()).isEqualTo(0);
        List<MessageSummary> inbox = join(mail.listFolder(user, Folder.INBOX, 10));
        assertThat(inbox.get(0).flags()).contains(Flag.READ);
    }

    @Test
    void concurrentMarkReadOnlyWinsOnce() {
        String user = "race-user@example.com";
        Message m = newMessage(user, "sender@example.com", "contended");
        join(mail.deliver(m));

        List<CompletableFuture<Boolean>> attempts = List.of(
                mail.markRead(user, Folder.INBOX, m.messageId()).toCompletableFuture(),
                mail.markRead(user, Folder.INBOX, m.messageId()).toCompletableFuture(),
                mail.markRead(user, Folder.INBOX, m.messageId()).toCompletableFuture());
        long wins = attempts.stream().map(CompletableFuture::join).filter(b -> b).count();

        assertThat(wins)
                .as("LWT must let exactly one concurrent markRead win the transition")
                .isEqualTo(1);
        assertThat(join(mail.getStats(user, Folder.INBOX)).unreadMessages()).isEqualTo(0);
    }

    @Test
    void moveToTrashAppliesTtlAndShiftsCounters() {
        String user = "trash-user@example.com";
        Message m = newMessage(user, "sender@example.com", "doomed");
        join(mail.deliver(m));

        assertThat(join(mail.moveToTrash(user, Folder.INBOX, m.messageId()))).isTrue();

        assertThat(join(mail.listFolder(user, Folder.INBOX, 10))).isEmpty();
        List<MessageSummary> trash = join(mail.listFolder(user, Folder.TRASH, 10));
        assertThat(trash).extracting(MessageSummary::subject).containsExactly("doomed");

        Row ttlRow = session.execute(
                "SELECT TTL(subject) AS ttl FROM mailstore.messages_by_mailbox "
                        + "WHERE user_id = ? AND folder = ? AND bucket = ? AND message_id = ?",
                user, Folder.TRASH.name(),
                TimeBuckets.bucketOf(m.messageId()), m.messageId()).one();
        assertThat(ttlRow).isNotNull();
        assertThat(ttlRow.getInt("ttl"))
                .as("trash listing row must carry the 30-day TTL")
                .isPositive()
                .isLessThanOrEqualTo(MessageRepository.TRASH_TTL_SECONDS);

        assertThat(join(mail.getStats(user, Folder.INBOX)).totalMessages()).isEqualTo(0);
        MailboxStats trashStats = join(mail.getStats(user, Folder.TRASH));
        assertThat(trashStats.totalMessages()).isEqualTo(1);
        assertThat(trashStats.unreadMessages()).isEqualTo(1);
    }

    @Test
    void listingWalksBackAcrossMonthBuckets() {
        String user = "bucket-user@example.com";
        Instant lastMonth = Instant.now().minus(Duration.ofDays(40));
        Message old = new Message(user, Uuids.startOf(lastMonth.toEpochMilli()),
                Folder.INBOX, "old@example.com", List.of(user),
                "from last month", "old body", 8, Set.of());
        Message recent = newMessage(user, "new@example.com", "from this month");
        join(mail.deliver(old));
        join(mail.deliver(recent));

        assertThat(TimeBuckets.bucketOf(old.messageId()))
                .as("precondition: the two messages land in different partitions")
                .isNotEqualTo(TimeBuckets.bucketOf(recent.messageId()));

        List<MessageSummary> inbox = join(mail.listFolder(user, Folder.INBOX, 10));
        assertThat(inbox).extracting(MessageSummary::subject)
                .containsExactly("from this month", "from last month");
    }

    private static Message newMessage(String user, String sender, String subject) {
        return new Message(user, Uuids.timeBased(), Folder.INBOX, sender,
                List.of(user), subject, "body of " + subject, 64, Set.of());
    }

    private static <T> T join(java.util.concurrent.CompletionStage<T> stage) {
        return stage.toCompletableFuture().orTimeout(30, java.util.concurrent.TimeUnit.SECONDS).join();
    }
}
