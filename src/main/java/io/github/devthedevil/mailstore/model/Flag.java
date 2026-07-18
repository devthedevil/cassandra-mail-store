package io.github.devthedevil.mailstore.model;

/** IMAP-style message flags, stored as a Cassandra {@code set<text>}. */
public enum Flag {
    READ,
    FLAGGED,
    ANSWERED
}
