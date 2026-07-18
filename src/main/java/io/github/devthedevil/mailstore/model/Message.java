package io.github.devthedevil.mailstore.model;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A full email message as stored in {@code messages_by_id}.
 *
 * @param messageId TIMEUUID — encodes the receive timestamp, used for ordering
 *                  and for deriving the partition time bucket.
 */
public record Message(
        String userId,
        UUID messageId,
        Folder folder,
        String sender,
        List<String> recipients,
        String subject,
        String body,
        int sizeBytes,
        Set<Flag> flags) {

    /** First 120 characters of the body, denormalized into the listing table. */
    public String preview() {
        if (body == null) {
            return "";
        }
        return body.length() <= 120 ? body : body.substring(0, 120);
    }
}
