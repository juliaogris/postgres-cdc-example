# PostgreSQL CDC Example

This project demonstrates Change Data Capture (CDC) and replication between two
PostgreSQL databases using two different approaches:

1. Manual CDC using wal2json
2. Native PostgreSQL logical replication (pub/sub)

## Components

1. **writer** - Continuously writes random data to the source database
2. **replicator** - Manual CDC implementation using wal2json for change capture
3. **pubsub** - Native PostgreSQL logical replication using publication/subscription
4. **PostgreSQL instances** - Two PostgreSQL databases with wal2json support

## Docker Commands to Run PostgreSQL Instances

    docker-compose up -d    # Start both databases with WAL2JSON extension
    docker-compose logs -f  # View logs
    docker-compose down     # Stop databases
    docker-compose down -v  # Stop and remove volumes (clean slate)

## Manual CDC with wal2json (replicator)

Start writer (Data Generator) in one terminal to create data in the source DB:

    go run ./writer

Start replicator in another terminal to consume changes from the source DB:

    go run ./replicator

- Manual parses of wal2json output
- Bulk copies existing data first
- Polls for changes every 2 seconds
- Full control over change processing

#### Native PostgreSQL Logical Replication (pubsub)

Start writer (Data Generator) in one terminal to create data in the source DB:

    go run ./writer

Start pubsub in another terminal to consume changes from the source DB:

    go run ./pubsub

- Native PostgreSQL logical replication
- Automatic change propagation
- Lower latency than polling
- Simpler and more reliable
- Built-in monitoring of replication status

## Verify Replication

Connect to both databases and check the data:

    docker exec -it postgres-source psql -U postgres -d testdb -c "SELECT COUNT(*) FROM person;"
    docker exec -it postgres-target psql -U postgres -d testdb -c "SELECT COUNT(*) FROM person;"

## Architecture

### Manual CDC with wal2json (replicator)
```
┌─────────────┐       ┌──────────────────┐       ┌──────────────┐
│  writer.go  │──────>│ Source PostgreSQL│<──────│ replicator.go│
│             │       │   (port 5429)    │       │              │
│ Writes      │       │   with wal2json  │       │ 1. Bulk copy │
│ random data │       └──────────────────┘       │ 2. Parse WAL │
│ every 1s    │                                  │ 3. Apply     │
└─────────────┘                                  │              │
                                                 │              │
                      ┌──────────────────┐       │              │
                      │ Target PostgreSQL│<──────│              │
                      │   (port 5431)    │       └──────────────┘
                      └──────────────────┘
```

### Native Logical Replication (pubsub)
```
┌─────────────┐       ┌──────────────────┐       ┌─────────────┐
│  writer.go  │──────>│ Source PostgreSQL│<──────│  pubsub.go  │
│             │       │   (port 5429)    │       │             │
│ Writes      │       │   PUBLICATION    │       │ 1. Creates  │
│ random data │       └──────────────────┘       │    pub/sub  │
│ every 1s    │              │                   │ 2. Bulk copy│
└─────────────┘              │                   │ 3. Monitors │
                             │                   └─────────────┘
                             │                         │
                             │ Logical                 │ Initial
                             │ Replication             │ Bulk Copy
                             │ (ongoing)               │
                             ↓                         ↓
                      ┌──────────────────┐
                      │ Target PostgreSQL│
                      │   (port 5431)    │
                      │   SUBSCRIPTION   │
                      └──────────────────┘
```

## Comparison: replicator vs pubsub

| Feature | replicator (wal2json) | pubsub (native) |
|---------|----------------------|-----------------|
| **Setup Complexity** | More complex - manual parsing | Simple - built-in feature |
| **Performance** | Polling-based (2s delay) | Real-time push |
| **Reliability** | Requires manual error handling | PostgreSQL handles retries |
| **Flexibility** | Full control over processing | Standard PostgreSQL behavior |
| **Use Case** | Custom transformations needed | Direct replication |
| **Maintenance** | Requires monitoring slot consumption | Self-managing |

## Important Notes

1. Both PostgreSQL instances are configured with:
   - `wal_level=logical`
   - wal2json extension installed
   - Sufficient replication slots

2. For the **replicator** approach:
   - Uses wal2json v2 format for parsing changes
   - Polls for changes every 2 seconds
   - Manual bulk copy before starting CDC
   - Requires careful management of replication slots

3. For the **pubsub** approach:
   - Uses native PostgreSQL logical replication
   - Automatic, real-time change propagation
   - PostgreSQL handles all the complexity
   - Better for production use cases

4. Docker networking:
   - Source PostgreSQL: port 5429
   - Target PostgreSQL: port 5431
   - Containers communicate via Docker network

## Troubleshooting

### Replication slot issues
If you get "replication slot already exists" error:
```sql
-- For replicator: Connect to source database and drop the slot
SELECT pg_drop_replication_slot('migration_slot');

-- For pubsub: Drop subscription first, then publication
-- On target:
DROP SUBSCRIPTION IF EXISTS person_subscription;
-- On source:
DROP PUBLICATION IF EXISTS person_publication;
```

### Check wal2json availability
```sql
-- Check if wal2json is available
SELECT * FROM pg_available_extensions WHERE name LIKE '%wal%';

-- Test creating a slot with wal2json
SELECT pg_create_logical_replication_slot('test_slot', 'wal2json');
SELECT pg_drop_replication_slot('test_slot');
```

### Monitor replication status
```sql
-- Check active replication slots
SELECT * FROM pg_replication_slots;

-- Check publication (on source)
SELECT * FROM pg_publication;

-- Check subscription (on target)
SELECT * FROM pg_subscription;

-- Check subscription status
SELECT * FROM pg_stat_subscription;
```
