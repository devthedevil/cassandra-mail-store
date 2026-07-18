-- Keyspace for local development / tests. Production would use
-- NetworkTopologyStrategy with RF=3 per datacenter.
CREATE KEYSPACE IF NOT EXISTS mailstore
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Query table: "list a folder, newest first".
-- Partition key (user_id, folder, bucket) keeps partitions bounded: bucket is
-- a yyyyMM month derived from the message's TIMEUUID, so even a very busy
-- mailbox never grows a single unbounded wide row. Clustering on
-- message_id DESC gives newest-first reads for free.
CREATE TABLE IF NOT EXISTS mailstore.messages_by_mailbox (
  user_id    text,
  folder     text,
  bucket     int,
  message_id timeuuid,
  sender     text,
  subject    text,
  preview    text,
  size_bytes int,
  flags      set<text>,
  PRIMARY KEY ((user_id, folder, bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- Query table: "open one message" — full body, denormalized from the
-- mailbox listing table (writes go to both in a logged batch).
CREATE TABLE IF NOT EXISTS mailstore.messages_by_id (
  user_id    text,
  message_id timeuuid,
  folder     text,
  sender     text,
  recipients list<text>,
  subject    text,
  body       text,
  size_bytes int,
  flags      set<text>,
  PRIMARY KEY ((user_id), message_id)
);

-- Distributed counters for cheap mailbox badges (total / unread) without
-- COUNT(*) scans. Counter columns must live in their own table.
CREATE TABLE IF NOT EXISTS mailstore.mailbox_stats (
  user_id         text,
  folder          text,
  total_messages  counter,
  unread_messages counter,
  PRIMARY KEY ((user_id), folder)
);
