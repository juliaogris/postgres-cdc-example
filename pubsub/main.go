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

	// Connect to source database
	sourcePool, err := pgxpool.New(ctx, sourceConnStr)
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer sourcePool.Close()

	// Connect to target database  
	targetPool, err := pgxpool.New(ctx, targetConnStr)
	if err != nil {
		log.Fatal("Failed to connect to target database:", err)
	}
	defer targetPool.Close()

	// Test connections
	if err := sourcePool.Ping(ctx); err != nil {
		log.Fatal("Failed to ping source database:", err)
	}
	if err := targetPool.Ping(ctx); err != nil {
		log.Fatal("Failed to ping target database:", err)
	}
	fmt.Println("Successfully connected to both databases!")

	// Step 1: Create table on target if it doesn't exist
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

	// Step 2: Drop existing publication and subscription if they exist
	fmt.Println("\nCleaning up existing replication objects...")
	
	// Drop subscription on target (must be done before dropping publication)
	dropSubSQL := `DROP SUBSCRIPTION IF EXISTS person_subscription`
	_, err = targetPool.Exec(ctx, dropSubSQL)
	if err != nil {
		log.Printf("Warning: Could not drop subscription: %v", err)
	}

	// Drop publication on source
	dropPubSQL := `DROP PUBLICATION IF EXISTS person_publication`
	_, err = sourcePool.Exec(ctx, dropPubSQL)
	if err != nil {
		log.Printf("Warning: Could not drop publication: %v", err)
	}

	// Step 3: Create publication on source database with WHERE clause for even scores
	fmt.Println("\nCreating publication on source database (only even scores)...")
	createPubSQL := `CREATE PUBLICATION person_publication FOR TABLE person WHERE (score % 2 = 0)`
	_, err = sourcePool.Exec(ctx, createPubSQL)
	if err != nil {
		log.Fatal("Failed to create publication:", err)
	}
	fmt.Println("Publication 'person_publication' created with filter: score % 2 = 0")

	// Step 4: Truncate target table before subscription
	fmt.Println("\nPreparing target table for replication...")
	_, err = targetPool.Exec(ctx, "TRUNCATE TABLE person RESTART IDENTITY")
	if err != nil {
		log.Fatal("Failed to truncate target table:", err)
	}
	fmt.Println("Target table truncated, ready for subscription")

	// Step 5: Create subscription on target database
	fmt.Println("\nCreating subscription on target database...")
	fmt.Println("This will automatically copy existing data with even scores from source...")
	
	// Create subscription with copy_data = true (default) to automatically sync initial data
	createSubSQL := `
		CREATE SUBSCRIPTION person_subscription 
		CONNECTION 'host=host.docker.internal port=5429 user=postgres password=postgres dbname=testdb' 
		PUBLICATION person_publication
		WITH (synchronous_commit = 'off')`
	// copy_data defaults to true, so PostgreSQL will automatically copy existing data
	
	_, err = targetPool.Exec(ctx, createSubSQL)
	if err != nil {
		// Try with container name if host.docker.internal doesn't work
		createSubSQL = `
			CREATE SUBSCRIPTION person_subscription 
			CONNECTION 'host=postgres-source port=5432 user=postgres password=postgres dbname=testdb' 
			PUBLICATION person_publication
			WITH (synchronous_commit = 'off')`
		
		_, err = targetPool.Exec(ctx, createSubSQL)
		if err != nil {
			log.Fatal("Failed to create subscription:", err)
		}
	}
	fmt.Println("Subscription 'person_subscription' created")
	fmt.Println("PostgreSQL is now copying initial data and will continue replicating changes...")

	// Step 6: Monitor replication status
	fmt.Println("\n✅ Logical replication is now active!")
	fmt.Println("Only records with EVEN scores will be replicated.")
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

		fmt.Printf("[%s] Status: %s | Source total: %d | Target: %d",
			time.Now().Format("15:04:05"),
			status,
			sourceCount,
			targetCount)
		
		// Count only even scores in source for comparison
		var sourceEvenCount int
		err = sourcePool.QueryRow(ctx, "SELECT COUNT(*) FROM person WHERE score % 2 = 0").Scan(&sourceEvenCount)
		if err != nil {
			log.Printf("Failed to get source even count: %v", err)
			sourceEvenCount = -1
		}
		
		if targetCount == sourceEvenCount {
			fmt.Printf(" ✓ In sync (even scores only: %d)\n", targetCount)
		} else if sourceEvenCount >= 0 {
			fmt.Printf(" ⟳ Syncing (target: %d, source even: %d)\n", targetCount, sourceEvenCount)
		} else {
			fmt.Println()
		}

		// Also check for replication lag
		var lag interface{}
		lagSQL := `
			SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::int AS lag_seconds
			WHERE pg_is_in_recovery()`
		
		err = targetPool.QueryRow(ctx, lagSQL).Scan(&lag)
		if err == nil && lag != nil {
			fmt.Printf("                Replication lag: %v seconds\n", lag)
		}
	}
}