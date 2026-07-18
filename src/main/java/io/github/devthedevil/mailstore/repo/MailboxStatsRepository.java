package io.github.devthedevil.mailstore.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import io.github.devthedevil.mailstore.model.Folder;
import io.github.devthedevil.mailstore.model.MailboxStats;

import java.util.concurrent.CompletionStage;

/**
 * Counter-table access for mailbox badges. Counters make "3,412 messages,
 * 17 unread" an O(1) read instead of a partition scan; the trade-off is that
 * counter updates are not idempotent, so callers must only adjust counts on
 * confirmed state transitions (see {@code MailService}).
 */
public final class MailboxStatsRepository {

    private final CqlSession session;
    private final PreparedStatement delivered;
    private final PreparedStatement read;
    private final PreparedStatement removeFrom;
    private final PreparedStatement addTo;
    private final PreparedStatement select;

    public MailboxStatsRepository(CqlSession session) {
        this.session = session;
        this.delivered = session.prepare(
                "UPDATE mailstore.mailbox_stats "
                        + "SET total_messages = total_messages + 1, unread_messages = unread_messages + 1 "
                        + "WHERE user_id = ? AND folder = ?");
        this.read = session.prepare(
                "UPDATE mailstore.mailbox_stats SET unread_messages = unread_messages - 1 "
                        + "WHERE user_id = ? AND folder = ?");
        this.removeFrom = session.prepare(
                "UPDATE mailstore.mailbox_stats "
                        + "SET total_messages = total_messages - 1, unread_messages = unread_messages - ? "
                        + "WHERE user_id = ? AND folder = ?");
        this.addTo = session.prepare(
                "UPDATE mailstore.mailbox_stats "
                        + "SET total_messages = total_messages + 1, unread_messages = unread_messages + ? "
                        + "WHERE user_id = ? AND folder = ?");
        this.select = session.prepare(
                "SELECT total_messages, unread_messages FROM mailstore.mailbox_stats "
                        + "WHERE user_id = ? AND folder = ?");
    }

    public CompletionStage<Void> recordDelivered(String userId, Folder folder) {
        return session.executeAsync(delivered.bind(userId, folder.name())).thenApply(rs -> null);
    }

    public CompletionStage<Void> recordRead(String userId, Folder folder) {
        return session.executeAsync(read.bind(userId, folder.name())).thenApply(rs -> null);
    }

    /** Adjusts both folders' counters atomically in a COUNTER batch. */
    public CompletionStage<Void> recordMoved(String userId, Folder from, Folder to, boolean wasUnread) {
        long unreadDelta = wasUnread ? 1L : 0L;
        BatchStatement batch = BatchStatement.builder(DefaultBatchType.COUNTER)
                .addStatement(removeFrom.bind(unreadDelta, userId, from.name()))
                .addStatement(addTo.bind(unreadDelta, userId, to.name()))
                .build();
        return session.executeAsync(batch).thenApply(rs -> null);
    }

    public CompletionStage<MailboxStats> getStats(String userId, Folder folder) {
        return session.executeAsync(select.bind(userId, folder.name()))
                .thenApply(rs -> {
                    Row row = rs.one();
                    long total = row == null ? 0 : row.getLong("total_messages");
                    long unread = row == null ? 0 : row.getLong("unread_messages");
                    return new MailboxStats(userId, folder, total, unread);
                });
    }
}
