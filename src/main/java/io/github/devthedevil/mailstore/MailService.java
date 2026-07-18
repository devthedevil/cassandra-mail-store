package io.github.devthedevil.mailstore;

import com.datastax.oss.driver.api.core.CqlSession;
import io.github.devthedevil.mailstore.model.Flag;
import io.github.devthedevil.mailstore.model.Folder;
import io.github.devthedevil.mailstore.model.MailboxStats;
import io.github.devthedevil.mailstore.model.Message;
import io.github.devthedevil.mailstore.model.MessageSummary;
import io.github.devthedevil.mailstore.repo.MailboxStatsRepository;
import io.github.devthedevil.mailstore.repo.MessageRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Application-facing facade that composes the message and counter
 * repositories into end-to-end asynchronous flows. Counter adjustments only
 * happen on confirmed state transitions (LWT applied, move found a message),
 * which keeps the non-idempotent counters honest.
 */
public final class MailService {

    private final MessageRepository messages;
    private final MailboxStatsRepository stats;

    public MailService(CqlSession session) {
        this.messages = new MessageRepository(session);
        this.stats = new MailboxStatsRepository(session);
    }

    /** Persists an incoming message and bumps the folder's badge counters. */
    public CompletionStage<Void> deliver(Message message) {
        return messages.save(message)
                .thenCompose(v -> stats.recordDelivered(message.userId(), message.folder()));
    }

    public CompletionStage<List<MessageSummary>> listFolder(String userId, Folder folder, int limit) {
        return messages.fetchRecent(userId, folder, limit);
    }

    public CompletionStage<Optional<Message>> open(String userId, UUID messageId) {
        return messages.findById(userId, messageId);
    }

    /** Marks read; decrements the unread badge only if this call won the transition. */
    public CompletionStage<Boolean> markRead(String userId, Folder folder, UUID messageId) {
        return messages.markRead(userId, folder, messageId)
                .thenCompose(transitioned -> transitioned
                        ? stats.recordRead(userId, folder).thenApply(v -> true)
                        : completedFuture(false));
    }

    /** Moves to TRASH (30-day TTL) and shifts both folders' counters. */
    public CompletionStage<Boolean> moveToTrash(String userId, Folder folder, UUID messageId) {
        return messages.moveToTrash(userId, folder, messageId)
                .thenCompose(moved -> {
                    if (moved.isEmpty()) {
                        return completedFuture(false);
                    }
                    boolean wasUnread = !moved.get().flags().contains(Flag.READ);
                    return stats.recordMoved(userId, folder, Folder.TRASH, wasUnread)
                            .thenApply(v -> true);
                });
    }

    public CompletionStage<MailboxStats> getStats(String userId, Folder folder) {
        return stats.getStats(userId, folder);
    }
}
