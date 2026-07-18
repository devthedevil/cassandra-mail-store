package io.github.devthedevil.mailstore.demo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.github.devthedevil.mailstore.MailService;
import io.github.devthedevil.mailstore.config.CqlSessionFactory;
import io.github.devthedevil.mailstore.model.Folder;
import io.github.devthedevil.mailstore.model.Message;
import io.github.devthedevil.mailstore.model.MessageSummary;
import io.github.devthedevil.mailstore.schema.SchemaInitializer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * End-to-end walkthrough against a local node ({@code docker compose up -d}):
 * delivers a batch of messages concurrently, lists the inbox, reads one,
 * trashes one, and prints the counter-backed badges.
 *
 * <p>Run with: {@code mvn compile exec:java}
 */
public final class MailStoreDemo {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        String user = "demo@icloud.example";

        try (CqlSession session = CqlSessionFactory.create(
                new InetSocketAddress(host, 9042), "datacenter1")) {

            SchemaInitializer.apply(session);
            MailService mail = new MailService(session);

            System.out.println("== Delivering 5 messages concurrently ==");
            List<CompletableFuture<Void>> deliveries = List.of(
                    deliver(mail, user, "alerts@bank.example", "Statement ready", "Your July statement is available."),
                    deliver(mail, user, "team@work.example", "Standup moved", "Standup is at 9:30 tomorrow."),
                    deliver(mail, user, "noreply@store.example", "Order shipped", "Your order #4521 has shipped."),
                    deliver(mail, user, "friend@mail.example", "Hiking Saturday?", "Trail conditions look great this weekend."),
                    deliver(mail, user, "news@daily.example", "Morning briefing", "Top stories for today..."));
            CompletableFuture.allOf(deliveries.toArray(CompletableFuture[]::new)).join();

            List<MessageSummary> inbox = mail.listFolder(user, Folder.INBOX, 10)
                    .toCompletableFuture().join();
            System.out.println("\n== Inbox (newest first) ==");
            inbox.forEach(m -> System.out.printf("  [%s] %-24s %s%n",
                    m.isRead() ? "read  " : "unread", m.sender(), m.subject()));

            UUID first = inbox.get(0).messageId();
            boolean transitioned = mail.markRead(user, Folder.INBOX, first)
                    .toCompletableFuture().join();
            System.out.printf("%n== markRead(%s) -> %s; second call -> %s (idempotent) ==%n",
                    first, transitioned,
                    mail.markRead(user, Folder.INBOX, first).toCompletableFuture().join());

            UUID second = inbox.get(1).messageId();
            mail.moveToTrash(user, Folder.INBOX, second).toCompletableFuture().join();
            System.out.println("== Moved one message to TRASH (expires in 30 days via TTL) ==");

            System.out.println("\n== Mailbox badges (counter tables) ==");
            for (Folder f : List.of(Folder.INBOX, Folder.TRASH)) {
                var s = mail.getStats(user, f).toCompletableFuture().join();
                System.out.printf("  %-6s total=%d unread=%d%n",
                        f, s.totalMessages(), s.unreadMessages());
            }
        }
    }

    private static CompletableFuture<Void> deliver(
            MailService mail, String user, String sender, String subject, String body) {
        Message message = new Message(user, Uuids.timeBased(), Folder.INBOX, sender,
                List.of(user), subject, body, body.length(), Set.of());
        return mail.deliver(message).toCompletableFuture();
    }
}
