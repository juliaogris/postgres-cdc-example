package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	host     = "localhost"
	port     = 5429
	user     = "postgres"
	password = "postgres"
	dbname   = "testdb"
)

func main() {
	ctx := context.Background()

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer pool.Close()

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS person (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		uid UUID NOT NULL,
		score INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = pool.Exec(ctx, createTableSQL)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Println("Table 'person' created or already exists")

	// Random data generation
	names := []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"}

	// Insert random data every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	counter := 0
	for range ticker.C {
		counter++

		name := names[rand.Intn(len(names))] + fmt.Sprintf("_%d", counter)
		uid := uuid.New()
		score := rand.Intn(100) + 1

		insertSQL := `INSERT INTO person (name, uid, score) VALUES ($1, $2, $3)`
		_, err := pool.Exec(ctx, insertSQL, name, uid, score)
		if err != nil {
			log.Printf("Failed to insert record: %v", err)
			continue
		}

		fmt.Printf("Inserted: Name=%s, UID=%s, Score=%d\n", name, uid, score)
	}
}
