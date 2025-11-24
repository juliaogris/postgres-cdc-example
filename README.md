# PostgreSQL CDC with wal2json Example

This project demonstrates Change Data Capture (CDC) between two PostgreSQL databases using wal2json.

## Components

1. **writer** - Continuously writes random data to the source database
2. **replicator** - Performs bulk copy and then continuous CDC replication to target database
3. **PostgreSQL instances** - Two PostgreSQL databases (source with wal2json, target without)

## Docker Commands to Run PostgreSQL Instances

  docker-compose up -d # Start both databases
  docker-compose logs -f # View logs
  docker-compose down # Stop databases
  docker-compose down -v # Stop and remove volumes (clean slate)

## Running the Go Programs

### 1. Install Dependencies
```bash
go mod download
```

### 2. Run the Writer Program
This will continuously write random data to the source database:
```bash
go run ./writer
```

### 3. Run the Replicator Program
In a separate terminal, run the replicator which will:
- Bulk copy existing data
- Set up CDC with wal2json
- Continuously replicate changes

```bash
go run ./replicator
```

## Verify Replication

Connect to both databases and check the data:

```bash
# Connect to source database
docker exec -it postgres-source psql -U postgres -d testdb -c "SELECT COUNT(*) FROM person;"

# Connect to target database
docker exec -it postgres-target psql -U postgres -d testdb -c "SELECT COUNT(*) FROM person;"

# Watch real-time changes in target
docker exec -it postgres-target psql -U postgres -d testdb -c "SELECT * FROM person ORDER BY id DESC LIMIT 10;"
```

## Architecture

```
┌─────────────┐       ┌──────────────────┐       ┌──────────────┐
│  writer.go  │──────>│ Source PostgreSQL│<──────│ replicator.go│
│             │       │   (port 5432)    │       │              │
│ Writes      │       │   with wal2json  │       │ 1. Bulk copy │
│ random data │       └──────────────────┘       │ 2. CDC       │
│ every 1s    │                                  │              │
└─────────────┘                                  │              │
                                                 │              │
                      ┌──────────────────┐       │              │
                      │ Target PostgreSQL│<──────│              │
                      │   (port 5433)    │       └──────────────┘
                      └──────────────────┘
```

## Important Notes

1. The source PostgreSQL must have:
   - `wal_level=logical`
   - wal2json extension installed
   - Sufficient replication slots

2. The replicator program uses a simplified CDC approach. For production:
   - Consider using `pglogrepl` library for better replication protocol handling
   - Implement proper error handling and retry logic
   - Handle schema changes appropriately
   - Use connection pooling

3. The bulk copy uses `ON CONFLICT DO NOTHING` to handle duplicate keys

4. CDC changes are polled every 2 seconds. Adjust based on your needs.

## Troubleshooting

If you get "replication slot already exists" error:
```sql
-- Connect to source database and drop the slot
SELECT pg_drop_replication_slot('migration_slot');
```

If wal2json is not available:
```sql
-- Check available output plugins
SELECT * FROM pg_available_extensions WHERE name LIKE '%wal%';
```

## Cleanup

```bash
# Stop and remove containers and volumes
docker-compose down -v

# Or if using individual containers
docker stop postgres-source postgres-target
docker rm postgres-source postgres-target
docker volume prune
```
