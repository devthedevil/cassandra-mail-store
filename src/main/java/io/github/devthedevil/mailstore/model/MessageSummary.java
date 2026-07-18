package io.github.devthedevil.mailstore.model;

import java.util.Set;
import java.util.UUID;

/** A folder-listing row from {@code messages_by_mailbox} — no body, just headers. */
public record MessageSummary(
        String userId,
        UUID messageId,
        Folder folder,
        String sender,
        String subject,
        String preview,
        int sizeBytes,
        Set<Flag> flags) {

    public boolean isRead() {
        return flags.contains(Flag.READ);
    }
}
