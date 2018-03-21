package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

const (
	dbHost          = "localhost"
	dbPort          = 5432
	defaultUser     = "postgres"
	defaultPassword = "postgres"
	defaultDB       = "postgres"
)

func main() {
	log.Print("Parser turned on")

	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = defaultUser
	}

	dbDBName := os.Getenv("DB_NAME")
	if dbDBName == "" {
		dbDBName = defaultDB
	}

	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		dbPassword = defaultPassword
	}

	db, err := sql.Open("postgres", fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", dbUser, dbPassword, dbDBName))
	if err != nil {
		log.Fatal(err)
	}

	parser, err := NewDataParser(db)
	if err != nil {
		log.Fatal(err)
	}

	fileListener, err := NewFileListener(*parser)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Listening..")
	defer fileListener.watcher.Close()

	fileListener.Run()
}
