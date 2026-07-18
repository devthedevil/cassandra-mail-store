package io.github.devthedevil.mailstore.schema;

import com.datastax.oss.driver.api.core.CqlSession;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Applies {@code schema.cql} (idempotent — every statement is IF NOT EXISTS). */
public final class SchemaInitializer {

    private static final String SCHEMA_RESOURCE = "/schema.cql";

    private SchemaInitializer() {
    }

    public static void apply(CqlSession session) {
        Arrays.stream(readSchema().split(";"))
                .map(SchemaInitializer::stripComments)
                .map(String::trim)
                .filter(stmt -> !stmt.isEmpty())
                .forEach(session::execute);
    }

    private static String readSchema() {
        try (InputStream in = SchemaInitializer.class.getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(SCHEMA_RESOURCE + " not found on classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String stripComments(String statement) {
        return statement.lines()
                .filter(line -> !line.trim().startsWith("--"))
                .reduce("", (a, b) -> a + "\n" + b);
    }
}
