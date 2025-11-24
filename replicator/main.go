package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Person struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	UID       uuid.UUID `json:"uid"`
	Score     int       `json:"score"`
	CreatedAt time.Time `json:"created_at"`
}

// WAL2JSON v2 format structures
type WAL2JSONColumn struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type WAL2JSONChange struct {
	Action    string           `json:"action"` // I for insert, U for update, D for delete
	Timestamp string           `json:"timestamp"`
	Schema    string           `json:"schema"`
	Table     string           `json:"table"`
	Columns   []WAL2JSONColumn `json:"columns"`
	Identity  []WAL2JSONColumn `json:"identity,omitempty"` // For updates and deletes
}

func main() {
	sourceConnStr := "host=localhost port=5429 user=postgres password=postgres dbname=testdb sslmode=disable"
	targetConnStr := "host=localhost port=5431 user=postgres password=postgres dbname=testdb sslmode=disable"

	ctx := context.Background()
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

	// Set up replication slot using wal2json plugin
	slotName := "migration_slot"
	var slotExists bool
	checkSlotSQL := `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`
	err = sourcePool.QueryRow(ctx, checkSlotSQL, slotName).Scan(&slotExists)
	if err != nil {
		log.Fatalf("Warning: Could not check if slot exists: %v", err)
	}

	if slotExists {
		dropSlotSQL := `SELECT pg_drop_replication_slot($1)`
		_, err = sourcePool.Exec(ctx, dropSlotSQL, slotName)
		if err != nil {
			log.Fatalf("Warning: Could not drop existing slot: %v", err)
		}
	}

	createSlotSQL := `SELECT pg_create_logical_replication_slot($1, 'wal2json')`
	_, err = sourcePool.Exec(ctx, createSlotSQL, slotName)
	if err != nil {
		log.Fatalf("Warning: Could not create replication slot (might already exist): %v", err)
	} else {
		fmt.Printf("Created replication slot: %s\n", slotName)
	}

	// Bulk copy existing data
	fmt.Println("\nStarting bulk copy of existing data...")

	rows, err := sourcePool.Query(ctx, `
		SELECT id, name, uid, score, created_at
		FROM person
		ORDER BY id`)
	if err != nil {
		log.Fatal("Failed to query source data:", err)
	}
	defer rows.Close()

	copiedCount := 0
	batch := &pgx.Batch{}

	for rows.Next() {
		var p Person
		err := rows.Scan(&p.ID, &p.Name, &p.UID, &p.Score, &p.CreatedAt)
		if err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		batch.Queue(`
			INSERT INTO person (id, name, uid, score, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO NOTHING`,
			p.ID, p.Name, p.UID, p.Score, p.CreatedAt)
		copiedCount++

		// Execute batch every 100 rows
		if batch.Len() >= 100 {
			br := targetPool.SendBatch(ctx, batch)
			if err := br.Close(); err != nil {
				log.Printf("Failed to execute batch: %v", err)
			}
			batch = &pgx.Batch{}
		}
	}
	if batch.Len() > 0 {
		br := targetPool.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			log.Printf("Failed to execute final batch: %v", err)
		}
	}
	fmt.Printf("Bulk copied %d records\n", copiedCount)

	// Update sequence to avoid conflicts
	var maxID int
	err = targetPool.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM person").Scan(&maxID)
	if err == nil && maxID > 0 {
		_, err = targetPool.Exec(ctx, fmt.Sprintf("ALTER SEQUENCE person_id_seq RESTART WITH %d", maxID+1))
		if err != nil {
			log.Printf("Warning: Could not update sequence: %v", err)
		}
	}

	// Poll for changes using pg_logical_slot_get_changes
	fmt.Println("\nStarting CDC (Change Data Capture)...")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Get changes from replication slot
		changesSQL := `
		SELECT data::text
		FROM pg_logical_slot_get_changes($1, NULL, NULL,
			'format-version', '2',
			'include-timestamp', 'true',
			'include-transaction', 'false')`

		changeRows, err := sourcePool.Query(ctx, changesSQL, slotName)
		if err != nil {
			log.Printf("Failed to get changes: %v", err)
			continue
		}

		fmt.Println("ticker", time.Now().Format("15:04:05"))

		processedChanges := 0
		for changeRows.Next() {
			fmt.Println("processing change", processedChanges)
			var changeData string
			if err := changeRows.Scan(&changeData); err != nil {
				log.Printf("Failed to scan change: %v", err)
				continue
			}

			// Parse wal2json output (v2 format - single object per line)
			var change WAL2JSONChange
			if err := json.Unmarshal([]byte(changeData), &change); err != nil {
				log.Printf("Failed to parse change JSON: %v", err)
				continue
			}

			fmt.Printf("CDC change: action=%s, table=%s\n", change.Action, change.Table)
			if change.Table != "person" {
				continue
			}

			switch change.Action {
			case "I": // Insert
				// Map column values
				values := make(map[string]any)
				for _, col := range change.Columns {
					values[col.Name] = col.Value
				}

				// Insert into target
				insertSQL := `
						INSERT INTO person (id, name, uid, score, created_at)
						VALUES ($1, $2, $3, $4, $5)
						ON CONFLICT (id) DO UPDATE SET
							name = EXCLUDED.name,
							uid = EXCLUDED.uid,
							score = EXCLUDED.score`

				_, err = targetPool.Exec(ctx, insertSQL,
					values["id"],
					values["name"],
					values["uid"],
					values["score"],
					values["created_at"])

				if err != nil {
					log.Printf("Failed to insert CDC record: %v", err)
				} else {
					fmt.Printf("CDC Insert: ID=%v, Name=%v\n", values["id"], values["name"])
					processedChanges++
				}

			case "U": // Update
				// Map column values
				values := make(map[string]any)
				for _, col := range change.Columns {
					values[col.Name] = col.Value
				}

				// Update target
				updateSQL := `
						UPDATE person
						SET name = $2, uid = $3, score = $4
						WHERE id = $1`

				_, err = targetPool.Exec(ctx, updateSQL,
					values["id"],
					values["name"],
					values["uid"],
					values["score"])

				if err != nil {
					log.Printf("Failed to update CDC record: %v", err)
				} else {
					fmt.Printf("CDC Update: ID=%v, Name=%v\n", values["id"], values["name"])
					processedChanges++
				}

			case "D": // Delete
				// Map identity values (primary key)
				values := make(map[string]any)
				for _, col := range change.Identity {
					values[col.Name] = col.Value
				}

				// Delete from target
				deleteSQL := `DELETE FROM person WHERE id = $1`
				_, err = targetPool.Exec(ctx, deleteSQL, values["id"])

				if err != nil {
					log.Printf("Failed to delete CDC record: %v", err)
				} else {
					fmt.Printf("CDC Delete: ID=%v\n", values["id"])
					processedChanges++
				}
			}
		}
		changeRows.Close()

		if processedChanges > 0 {
			fmt.Printf("Processed %d CDC changes\n", processedChanges)
		}
	}
}
