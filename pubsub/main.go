package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	// Connection strings
	sourceConnStr := "host=localhost port=5429 user=postgres password=postgres dbname=testdb sslmode=disable"
	targetConnStr := "host=localhost port=5431 user=postgres password=postgres dbname=testdb sslmode=disable"

	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer sourcePool.Close()

	targetPool, err := pgxpool.New(ctx, targetConnStr)
	if err != nil {
		log.Fatal("Failed to connect to target database:", err)
	}
	defer targetPool.Close()

	// Create table on target if it doesn't exist
	fmt.Println("\nEnsuring target table exists...")
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS person (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		uid UUID NOT NULL,
		score INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	_, err = targetPool.Exec(ctx, createTableSQL)
	if err != nil {
		log.Fatal("Failed to create target table:", err)
	}
	fmt.Println("Target table 'person' is ready")

	// Drop existing publication and subscription if they exist
	dropSubSQL := `DROP SUBSCRIPTION IF EXISTS person_subscription`
	_, err = targetPool.Exec(ctx, dropSubSQL)
	if err != nil {
		log.Printf("Warning: Could not drop subscription: %v", err)
	}
	dropPubSQL := `DROP PUBLICATION IF EXISTS person_publication`
	_, err = sourcePool.Exec(ctx, dropPubSQL)
	if err != nil {
		log.Printf("Warning: Could not drop publication: %v", err)
	}

	// Create publication on source database
	fmt.Println("\nCreating publication on source database...")
	createPubSQL := `CREATE PUBLICATION person_publication FOR TABLE person`
	_, err = sourcePool.Exec(ctx, createPubSQL)
	if err != nil {
		log.Fatal("Failed to create publication:", err)
	}
	fmt.Println("Publication 'person_publication' created")

	// Initial data sync - truncate target and copy from source
	fmt.Println("\nPerforming initial data sync...")
	_, err = targetPool.Exec(ctx, "TRUNCATE TABLE person RESTART IDENTITY")
	if err != nil {
		log.Fatal("Failed to truncate target table:", err)
	}
	copyCount := 0
	rows, err := sourcePool.Query(ctx, `SELECT id, name, uid, score, created_at FROM person ORDER BY id`)
	if err != nil {
		log.Fatal("Failed to query source data:", err)
	}
	defer rows.Close()

	// Use CopyFrom for efficient bulk insert
	columnNames := []string{"id", "name", "uid", "score", "created_at"}
	var copyData [][]any

	for rows.Next() {
		var id int
		var name string
		var uid, score any
		var createdAt time.Time

		err := rows.Scan(&id, &name, &uid, &score, &createdAt)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		copyData = append(copyData, []any{id, name, uid, score, createdAt})
		copyCount++
	}

	if len(copyData) > 0 {
		copyCount, err := targetPool.CopyFrom(
			ctx,
			pgx.Identifier{"person"},
			columnNames,
			pgx.CopyFromRows(copyData),
		)
		if err != nil {
			log.Fatal("Failed to copy data to target:", err)
		}
		fmt.Printf("Initial sync completed: %d records copied\n", copyCount)
	} else {
		fmt.Println("No records to copy in initial sync")
	}

	// Update sequence on target
	var maxID int
	err = targetPool.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM person").Scan(&maxID)
	if err == nil && maxID > 0 {
		_, err = targetPool.Exec(ctx, fmt.Sprintf("ALTER SEQUENCE person_id_seq RESTART WITH %d", maxID+1))
		if err != nil {
			log.Printf("Warning: Could not update sequence: %v", err)
		}
	}

	// Create subscription on target database
	fmt.Println("\nCreating subscription on target database...")

	// Create subscription with copy_data = false since we already copied the initial data
	createSubSQL := `
		CREATE SUBSCRIPTION person_subscription
		CONNECTION 'host=host.docker.internal port=5429 user=postgres password=postgres dbname=testdb'
		PUBLICATION person_publication
		WITH (copy_data = false, synchronous_commit = 'off')`

	_, err = targetPool.Exec(ctx, createSubSQL)
	if err != nil {
		// Try with localhost if host.docker.internal doesn't work
		createSubSQL = `
			CREATE SUBSCRIPTION person_subscription
			CONNECTION 'host=postgres-source port=5432 user=postgres password=postgres dbname=testdb'
			PUBLICATION person_publication
			WITH (copy_data = false, synchronous_commit = 'off')`

		_, err = targetPool.Exec(ctx, createSubSQL)
		if err != nil {
			log.Fatal("Failed to create subscription:", err)
		}
	}
	fmt.Println("Subscription 'person_subscription' created")

	// Monitor replication status
	fmt.Println("\nLogical replication is now active!")
	fmt.Println("The target database will automatically receive all changes from the source.")
	fmt.Println("\nMonitoring replication status...")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Check subscription status
		var subName, status string

		statusSQL := `
			SELECT subname, subenabled,
			       subconninfo
			FROM pg_subscription
			WHERE subname = 'person_subscription'
			LIMIT 1`

		var enabled bool
		var connInfo string

		err := targetPool.QueryRow(ctx, statusSQL).Scan(
			&subName, &enabled, &connInfo)

		if err != nil {
			if err == pgx.ErrNoRows {
				log.Println("Subscription not found")
			} else {
				log.Printf("Failed to check subscription status: %v", err)
			}
			continue
		}

		// Check row counts
		var sourceCount, targetCount int
		err = sourcePool.QueryRow(ctx, "SELECT COUNT(*) FROM person").Scan(&sourceCount)
		if err != nil {
			log.Printf("Failed to get source count: %v", err)
			continue
		}

		err = targetPool.QueryRow(ctx, "SELECT COUNT(*) FROM person").Scan(&targetCount)
		if err != nil {
			log.Printf("Failed to get target count: %v", err)
			continue
		}

		// Set status based on enabled state
		if enabled {
			status = "enabled (replicating)"
		} else {
			status = "disabled"
		}

		fmt.Printf("[%s] Status: %s | Source rows: %d | Target rows: %d",
			time.Now().Format("15:04:05"),
			status,
			sourceCount,
			targetCount)

		if sourceCount == targetCount {
			fmt.Println("In sync")
		} else {
			fmt.Printf("Syncing (%d behind)\n", sourceCount-targetCount)
		}

		// Also check for replication lag
		var lag any
		lagSQL := `
			SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::int AS lag_seconds
			WHERE pg_is_in_recovery()`

		err = targetPool.QueryRow(ctx, lagSQL).Scan(&lag)
		if err == nil && lag != nil {
			fmt.Printf("                Replication lag: %v seconds\n", lag)
		}
	}
}
