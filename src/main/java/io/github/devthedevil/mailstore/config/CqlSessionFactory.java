package io.github.devthedevil.mailstore.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * Builds {@link CqlSession} instances with sane defaults for this service.
 *
 * <p>The driver is fully non-blocking (Netty under the hood); one session is a
 * long-lived, thread-safe object that multiplexes all requests over a small
 * number of connections — create it once per application.
 */
public final class CqlSessionFactory {

    private CqlSessionFactory() {
    }

    public static CqlSession create(InetSocketAddress contactPoint, String localDatacenter) {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                // Generous timeout: schema DDL and first-contact metadata
                // refresh on a cold single-node cluster can exceed the 2s default.
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(15))
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                .build();

        return CqlSession.builder()
                .addContactPoint(contactPoint)
                .withLocalDatacenter(localDatacenter)
                .withConfigLoader(loader)
                .build();
    }
}
