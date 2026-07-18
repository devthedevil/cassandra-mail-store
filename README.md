# cassandra-mail-store

A distributed email message store built on **Apache Cassandra 5**, modeled after the
storage layer of a large-scale mail service. It demonstrates production-grade Cassandra
data modeling and a fully **asynchronous, non-blocking** Java data-access layer, verified
by integration tests that run against a **real Cassandra node** (Testcontainers).

[![CI](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

## What this project demonstrates

| Concern | Where | Technique |
|---|---|---|
| Query-first data modeling | [`schema.cql`](src/main/resources/schema.cql) | One table per access pattern (`messages_by_mailbox`, `messages_by_id`), denormalized on write |
| Bounded partitions | [`TimeBuckets`](src/main/java/io/github/devthedevil/mailstore/util/TimeBuckets.java) | Month bucket in the partition key — a hot mailbox can never grow an unbounded wide row |
| Async / non-blocking I/O | [`MessageRepository`](src/main/java/io/github/devthedevil/mailstore/repo/MessageRepository.java) | Every operation is `CompletionStage`-based over the driver's Netty core; multi-bucket listings compose async queries without blocking any thread |
| Concurrency control | `MessageRepository.markRead` | Lightweight transactions (`IF flags = ?`) as optimistic locking with bounded retry — two devices racing on flags can't lose writes |
| Multi-view write consistency | `MessageRepository.save` / `moveToTrash` | LOGGED batches keep the two denormalized tables in agreement |
| O(1) mailbox badges | [`MailboxStatsRepository`](src/main/java/io/github/devthedevil/mailstore/repo/MailboxStatsRepository.java) | Counter tables; counters only adjusted on confirmed state transitions because counter writes are not idempotent |
| Data lifecycle | `moveToTrash` | 30-day **TTL** on trash rows — Cassandra expires them, no purge job |
| Testing against the real database | [`MailServiceIT`](src/test/java/io/github/devthedevil/mailstore/MailServiceIT.java) | Testcontainers spins up Cassandra 5 in Docker; covers ordering, idempotency, LWT races, TTL, and counter accuracy |
| CI | [`ci.yml`](.github/workflows/ci.yml) | GitHub Actions runs the full suite, containers included, on every push |

## Data model

```
messages_by_mailbox              -- "list a folder, newest first"
  PK: ((user_id, folder, bucket), message_id DESC)
      bucket = yyyyMM derived from the TIMEUUID message id

messages_by_id                   -- "open one message" (full body)
  PK: ((user_id), message_id)

mailbox_stats                    -- badge counts without COUNT(*) scans
  PK: ((user_id), folder)        -- counter columns: total_messages, unread_messages
```

Both message tables are written in one LOGGED batch, so the listing view and the
canonical view converge even if a coordinator dies mid-write. The month bucket is
always recomputed from the TIMEUUID, so any component can locate a message's
listing row from its id alone.

## Requirements

- Java 17+
- Maven 3.9+
- Docker (for integration tests and the local node)

## Run the tests

```bash
mvn verify          # unit tests + Testcontainers integration tests (needs Docker)
mvn test            # unit tests only (no Docker needed)
```

## Run the demo

```bash
docker compose up -d --wait     # local Cassandra 5 on :9042
mvn compile exec:java           # deliver, list, read, trash, show badges
```

Expected output:

```
== Delivering 5 messages concurrently ==

== Inbox (newest first) ==
  [unread] news@daily.example       Morning briefing
  [unread] friend@mail.example      Hiking Saturday?
  ...

== markRead(...) -> true; second call -> false (idempotent) ==
== Moved one message to TRASH (expires in 30 days via TTL) ==

== Mailbox badges (counter tables) ==
  INBOX  total=4 unread=3
  TRASH  total=1 unread=1
```

## Design notes

- **Why a TIMEUUID as the message id?** It encodes the receive timestamp, giving
  free newest-first clustering order, uniqueness without coordination, and a way to
  derive the partition bucket from the id itself.
- **Why LWT for `markRead` but not for delivery?** Delivery is naturally idempotent
  (re-inserting the same row is harmless). Flag updates are read-modify-write on a
  set, so an unconditional write could silently drop a concurrent flag change; the
  LWT compare-and-set with retry makes the transition exactly-once — which also
  keeps the (non-idempotent) unread counter accurate.
- **Production deltas.** A real deployment would use `NetworkTopologyStrategy`
  (RF=3), mTLS between service and cluster, per-user rate limiting, and would size
  buckets from observed write rates (week buckets for very hot mailboxes).
