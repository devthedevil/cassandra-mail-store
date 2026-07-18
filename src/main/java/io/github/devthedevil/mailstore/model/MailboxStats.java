package io.github.devthedevil.mailstore.model;

/** Counter-backed badge numbers for one folder of one user. */
public record MailboxStats(String userId, Folder folder, long totalMessages, long unreadMessages) {
}
