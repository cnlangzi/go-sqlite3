//go:build cgo

package sqlite3

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// walHookTestSeq numbers the driver names registered by the tests below so
// that repeated runs (-count=2) do not register the same name twice.
var walHookTestSeq int32

// countLiveHandles returns the number of live entries in the package level
// handle map. It is not called countHandles because
// sqlite3_opt_vtable_leak_test.go already defines that name under the
// sqlite_vtable build tag, and CI compiles both files together.
func countLiveHandles() int {
	n := 0
	handleVals.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// countWalHookHandles returns the number of live handles that hold a WAL
// hook callback. Every open connection also registers the user
// authentication functions, so counting the whole map would not tell
// anything about the WAL hook alone.
func countWalHookHandles() int {
	n := 0
	handleVals.Range(func(_, v any) bool {
		if _, ok := v.(handleVal).val.(func(string, int) int); ok {
			n++
		}
		return true
	})
	return n
}

// walHookTestDSN returns a DSN for a fresh database in WAL mode.
func walHookTestDSN(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "walhook.db") + "?_journal_mode=WAL"
}

// openWalHookConn opens a database through the driver and returns the
// connection behind it.
func openWalHookConn(t *testing.T, dsn string) *SQLiteConn {
	t.Helper()
	raw, err := (&SQLiteDriver{}).Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	conn, ok := raw.(*SQLiteConn)
	if !ok {
		raw.Close()
		t.Fatal("driver did not return a *SQLiteConn")
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// openWalHookSQL opens a database through database/sql and returns the
// handle together with the connection behind it. The pool is limited to a
// single connection so that the returned connection is the one the tests
// write through.
func openWalHookSQL(t *testing.T, dsn string) (*sql.DB, *SQLiteConn) {
	t.Helper()
	name := fmt.Sprintf("sqlite3_walhook_%d", atomic.AddInt32(&walHookTestSeq, 1))
	var conn *SQLiteConn
	sql.Register(name, &SQLiteDriver{
		ConnectHook: func(c *SQLiteConn) error {
			conn = c
			return nil
		},
	})
	db, err := sql.Open(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	// Force the pool to open its connection so that ConnectHook has run.
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("ConnectHook was not called")
	}
	return db, conn
}

func TestRegisterWalHookFiresOnCommit(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var names []string
	var pages []int
	conn.RegisterWalHook(func(dbName string, nPages int) int {
		mu.Lock()
		defer mu.Unlock()
		names = append(names, dbName)
		pages = append(pages, nPages)
		return 0
	})

	if _, err := db.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(names) != 1 {
		t.Fatalf("WAL hook called %d times, want 1", len(names))
	}
	if names[0] != "main" {
		t.Fatalf("dbName = %q, want %q", names[0], "main")
	}
	if pages[0] <= 0 {
		t.Fatalf("nPages = %d, want > 0", pages[0])
	}
}

func TestRegisterWalHookIsConnectionLocal(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	calls := 0
	conn.RegisterWalHook(func(string, int) int {
		mu.Lock()
		calls++
		mu.Unlock()
		return 0
	})

	// A commit on a second connection has its own WAL hook state and must
	// not reach the hook of the first connection.
	other := openWalHookConn(t, dsn)
	if _, err := other.Exec("INSERT INTO t (id) VALUES (1)", nil); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("WAL hook called %d times for a commit on another connection, want 0", calls)
	}
}

func TestRegisterWalHookReplacesPreviousHook(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	baseline := countWalHookHandles()
	var mu sync.Mutex
	var first, second int
	conn.RegisterWalHook(func(string, int) int {
		mu.Lock()
		first++
		mu.Unlock()
		return 0
	})
	if got := countWalHookHandles(); got != baseline+1 {
		t.Fatalf("handle count after registration = %d, want %d", got, baseline+1)
	}
	conn.RegisterWalHook(func(string, int) int {
		mu.Lock()
		second++
		mu.Unlock()
		return 0
	})
	if got := countWalHookHandles(); got != baseline+1 {
		t.Fatalf("handle count after replacement = %d, want %d", got, baseline+1)
	}

	if _, err := db.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if first != 0 {
		t.Fatalf("replaced hook called %d times, want 0", first)
	}
	if second != 1 {
		t.Fatalf("replacement hook called %d times, want 1", second)
	}
}

func TestRegisterWalHookNilRemovesHook(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	baseline := countWalHookHandles()
	var mu sync.Mutex
	calls := 0
	conn.RegisterWalHook(func(string, int) int {
		mu.Lock()
		calls++
		mu.Unlock()
		return 0
	})
	conn.RegisterWalHook(nil)
	if got := countWalHookHandles(); got != baseline {
		t.Fatalf("handle count after removal = %d, want %d", got, baseline)
	}

	if _, err := db.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("WAL hook called %d times after removal, want 0", calls)
	}
}

func TestRegisterWalHookReleasesHandleOnClose(t *testing.T) {
	dsn := walHookTestDSN(t)
	baseline := countLiveHandles()
	conn := openWalHookConn(t, dsn)
	conn.RegisterWalHook(func(string, int) int { return 0 })
	if got := countWalHookHandles(); got != 1 {
		t.Fatalf("WAL hook handle count after registration = %d, want 1", got)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if got := countWalHookHandles(); got != 0 {
		t.Fatalf("WAL hook handle count after close = %d, want 0", got)
	}
	if got := countLiveHandles(); got != baseline {
		t.Fatalf("handle count after close = %d, want %d", got, baseline)
	}
}

func TestRegisterWalHookAfterCloseIsNoOp(t *testing.T) {
	dsn := walHookTestDSN(t)
	baseline := countLiveHandles()
	conn := openWalHookConn(t, dsn)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	// The connection is closed: registering a hook must not panic and must
	// not touch the freed sqlite3 handle.
	conn.RegisterWalHook(func(string, int) int { return 0 })
	if got := countWalHookHandles(); got != 0 {
		t.Fatalf("WAL hook handle count after registering on a closed connection = %d, want 0", got)
	}
	if got := countLiveHandles(); got != baseline {
		t.Fatalf("handle count after registering on a closed connection = %d, want %d", got, baseline)
	}
}

func TestRegisterWalHookCallbackPanic(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	const sentinel = "wal hook panic"
	conn.RegisterWalHook(func(string, int) int {
		panic(sentinel)
	})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		if _, err := db.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
			t.Errorf("Exec returned an error instead of the callback panic: %v", err)
		}
	}()
	if recovered == nil {
		t.Fatal("WAL hook panic did not reach the caller")
	}
	if got := fmt.Sprint(recovered); got != sentinel {
		t.Fatalf("panic value = %v, want %q", recovered, sentinel)
	}

	// The commit still completed.
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("row count = %d, want 1", n)
	}
}

func TestRegisterWalHookNonZeroReturn(t *testing.T) {
	dsn := walHookTestDSN(t)
	db, conn := openWalHookSQL(t, dsn)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	conn.RegisterWalHook(func(string, int) int { return 1 })

	if _, err := db.Exec("INSERT INTO t (id) VALUES (1)"); err == nil {
		t.Fatal("Exec succeeded, want an error from the WAL hook")
	}

	// The commit still completed.
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("row count = %d, want 1", n)
	}
}
