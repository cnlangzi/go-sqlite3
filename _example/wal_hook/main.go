package main

import (
	"database/sql"
	"log"
	"os"

	sqlite3 "github.com/cnlangzi/go-sqlite3"
)

func main() {
	os.Remove("./walhook.db")

	// The WAL hook is registered per connection and fires after every
	// commit in WAL mode. Here it stands in for a capture process that has
	// to be woken up whenever new data was committed.
	sql.Register("sqlite3_with_wal_hook",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.RegisterWalHook(func(dbName string, nPages int) int {
					log.Printf("commit on %q: %d pages in the write-ahead log", dbName, nPages)
					// Returning a non-zero value would make the statement
					// that provoked the commit report an error. Return 0
					// (SQLITE_OK) to keep the commit successful.
					return 0
				})
				return nil
			},
		})

	db, err := sql.Open("sqlite3_with_wal_hook", "./walhook.db?_journal_mode=WAL")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("create table foo(id int, value text)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("insert into foo values(1, 'foo')"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("insert into foo values(2, 'bar')"); err != nil {
		log.Fatal(err)
	}

	// Registering a WAL hook disables SQLite's automatic checkpointing, so
	// the write-ahead log has to be checkpointed from time to time. The
	// checkpoint runs on a second connection: the callback above runs
	// inside the commit path of its own connection and must not call back
	// into that connection.
	checkpoint, err := sql.Open("sqlite3_with_wal_hook", "./walhook.db?_journal_mode=WAL")
	if err != nil {
		log.Fatal(err)
	}
	defer checkpoint.Close()

	if _, err := checkpoint.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		log.Fatal(err)
	}
}
