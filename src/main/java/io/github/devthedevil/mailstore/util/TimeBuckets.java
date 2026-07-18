package io.github.devthedevil.mailstore.util;

import com.datastax.oss.driver.api.core.uuid.Uuids;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Month-based time bucketing (yyyyMM as an int, e.g. 202607).
 *
 * <p>Bucketing bounds partition size: a mailbox folder partition holds at most
 * one month of messages, so hot mailboxes cannot grow an unbounded wide row.
 * The bucket is always derived from the message's TIMEUUID, so any node can
 * recompute the partition key from the message id alone.
 */
public final class TimeBuckets {

    private TimeBuckets() {
    }

    public static int bucketOf(Instant instant) {
        LocalDate date = instant.atZone(ZoneOffset.UTC).toLocalDate();
        return date.getYear() * 100 + date.getMonthValue();
    }

    public static int bucketOf(UUID timeUuid) {
        return bucketOf(Instant.ofEpochMilli(Uuids.unixTimestamp(timeUuid)));
    }

    /** The bucket immediately before {@code bucket} (202601 -> 202512). */
    public static int previous(int bucket) {
        int year = bucket / 100;
        int month = bucket % 100;
        return month == 1 ? (year - 1) * 100 + 12 : year * 100 + (month - 1);
    }
}
