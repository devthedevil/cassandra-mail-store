package io.github.devthedevil.mailstore;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import io.github.devthedevil.mailstore.util.TimeBuckets;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TimeBucketsTest {

    @Test
    void bucketIsYearMonthInUtc() {
        assertThat(TimeBuckets.bucketOf(Instant.parse("2026-07-17T10:15:30Z"))).isEqualTo(202607);
        assertThat(TimeBuckets.bucketOf(Instant.parse("2026-01-01T00:00:00Z"))).isEqualTo(202601);
        assertThat(TimeBuckets.bucketOf(Instant.parse("2025-12-31T23:59:59Z"))).isEqualTo(202512);
    }

    @Test
    void bucketOfTimeUuidMatchesItsTimestamp() {
        UUID id = Uuids.timeBased();
        int expected = TimeBuckets.bucketOf(Instant.ofEpochMilli(Uuids.unixTimestamp(id)));
        assertThat(TimeBuckets.bucketOf(id)).isEqualTo(expected);
    }

    @Test
    void previousBucketHandlesYearRollover() {
        assertThat(TimeBuckets.previous(202607)).isEqualTo(202606);
        assertThat(TimeBuckets.previous(202601)).isEqualTo(202512);
    }
}
