package pool

import (
	"errors"
	"fmt"
	"github.com/ziutek/mymysql/mysql"
	"io"
	"time"
)

// Pool-specific errors
var (
	ErrCollationWithoutCharset = errors.New("Can't set collation without setting charset")
	ErrConnectionNotInPool     = errors.New("Connection not associated with a pool")
	ErrRequestTimeout          = errors.New("Query took too long to execute")
)

// A Conn is a database connection that belongs to a pool.
type Conn struct {
	mysql.Conn
	pool       *Pool
	statements map[string]*Stmt
	expiryDate time.Time
}

// Release replaces a connection into its pool.
func (conn *Conn) Release() error {
	if conn.pool == nil {
		return ErrConnectionNotInPool
	}
	if conn.pool.config.KeepConnectionsAlive {
		if conn.verify() {
			select {
			case conn.pool.idleConnections <- conn:
			default:
				// Destroy the connection if the idleConnections channel is full
				conn.Destroy()
			}
			return nil
		}
	}
	conn.Destroy()
	return nil
}

// Destroy closes the connection and removes it from its pool.  A connection
// must NOT be used after it has been destroyed; doing so will cause a panic.
func (conn *Conn) Destroy() {
	if conn.Conn.IsConnected() {
		conn.Conn.Close()
	}

	if conn.pool != nil {
		pool := conn.pool
		pool.mutex.Lock()
		defer pool.mutex.Unlock()
		delete(pool.openConnections, conn)
		conn.statements = map[string]*Stmt{}
		conn.pool = nil

		if pool.numPending > 0 {
			if newConn, err := pool.createConn(); err == nil {
				pool.idleConnections <- newConn
			}
		}
	}
}

// Connect opens the connection.
func (conn *Conn) Connect() error {
	if err := conn.Conn.Connect(); err != nil {
		return err
	}

	return conn.prepareConnection()
}

// Reconnect closes and reopens the connection.
func (conn *Conn) Reconnect() error {
	if err := conn.Conn.Reconnect(); err != nil {
		return err
	}

	return conn.prepareConnection()
}

// Prepare returns a prepared statement for the given SQL.  Prepared statements
// are cached so that a statement is only prepared the first time it is used on
// a particular connection.  The time allowed for the statement to be prepared
// is limited according to the pool's request timeout.
func (conn *Conn) Prepare(sql string) (stmt mysql.Stmt, err error) {
	var ok bool
	if stmt, ok = conn.statements[sql]; !ok {
		err = conn.withTimeout(func() error {
			return conn.destroyOnError(func() error {
				raw, e := conn.Conn.Prepare(sql)
				if e == nil {
					s := &Stmt{raw, conn, sql}
					conn.statements[sql] = s
					stmt = s
				}
				return e
			})
		})
	}

	return
}

// withTimeout executes a function but allows only the given amount of time for it to complete.
func (conn *Conn) withTimeout(f func() error) (err error) {
	op := make(chan bool, 1)
	go func() {
		err = f()
		op <- true
	}()
	select {
	case <-op:
		return
	case <-time.After(conn.pool.requestTimeout):
		// close connection which also cancels the query on the DB server
		conn.Close()
		return ErrRequestTimeout
	}
}

// destroyOnError destroys the connection if the given function returns an error
// that is:
//   - A non-MySQL error other than io.EOF
//   - A MySQL error with a code greater than 2000 (a client error)
//   - A MySQL error that indicates a network failure
//   - A MySQL error that indicates that the server has run out of memory, disk space, etc.
//   - A MySQL error that indicates that the server is misconfigured, corrupt, or unstable
func (conn *Conn) destroyOnError(f func() error) error {
	err := f()
	if err != nil {
		if mysqlErr, ok := err.(*mysql.Error); ok {
			switch mysqlErr.Code {
			case
				1021, // Disk is full
				1037, // Server is out of memory and needs to be restarted
				1041, // Server is out of memory
				1042, // Can't get hostname
				1043, // Bad handshake
				1044, // Access denied to database
				1045, // Access denied
				1053, // Server shutdown in progress
				1077, // Normal shutdown
				1078, // Aborting because of signal
				1079, // Shutdown complete
				1080, // Forcing thread to close
				1081, // Can't create IP socket
				1114, // Table is full
				1119, // Thread stack overrun
				1152, // Aborting connection
				1153, // Network packet too large
				1154, // Read error from pipe
				1155, // Error from fcntl()
				1156, // Network packets out of order
				1157, // Couldn't decompress packet
				1158, // Error reading network packets
				1159, // Timeout when reading packets
				1160, // Error writing network packets
				1161, // Timeout when writing packets
				1188, // Error from master
				1189, // Network error reading from master
				1190, // Network error writing to master
				1194, // Table has crashed and requires repair
				1195, // Table has crashed and repair failed
				1197, // Transaction cache is full
				1203, // User has too many connections
				1218, // Error connecting to master
				1219, // Error running query on master
				1436, // Thread stack overrun
				1459, // Table upgrade required
				1534, // Writing to binlog failed
				1535, // Table definitions on master and slave don't match
				1547, // Column count wrong; table is probably corrupted
				1548, // Table is probably corrupted
				1610, // Corrupted replication statement
				1705: // Statement cache is full
				conn.Destroy()
			default:
				if mysqlErr.Code >= 2000 {
					conn.Destroy()
				}
			}
		} else if err != io.EOF {
			conn.Destroy()
		}
	}
	return err
}

// Query executes a query on a connection.
// The execution time is limited according to the pool's request timeout.
func (conn *Conn) Query(sql string, params ...interface{}) (rows []mysql.Row, result mysql.Result, err error) {
	conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			rows, result, err = conn.Conn.Query(sql, params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, conn}
	}
	return
}

// QueryFirst executes a query on a connection, returning only the first row in
// the result set.  The execution time is limited according to the pool's
// request timeout.
func (conn *Conn) QueryFirst(sql string, params ...interface{}) (row mysql.Row, result mysql.Result, err error) {
	conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			row, result, err = conn.Conn.QueryFirst(sql, params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, conn}
	}
	return
}

// QueryLast executes a query on a connection, returning only the last row in
// the result set.  The execution time is limited according to the pool's
// request timeout.
func (conn *Conn) QueryLast(sql string, params ...interface{}) (row mysql.Row, result mysql.Result, err error) {
	conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			row, result, err = conn.Conn.QueryLast(sql, params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, conn}
	}
	return
}

// Start initiates a new query.
func (conn *Conn) Start(sql string, params ...interface{}) (result mysql.Result, err error) {
	conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			result, err = conn.Conn.Start(sql, params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, conn}
	}
	return
}

// Begin initiates a new transaction.
func (conn *Conn) Begin() (trans mysql.Transaction, err error) {
	conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			trans, err = conn.Conn.Begin()
			return err
		})
	})
	if err == nil {
		trans = &Transaction{conn, trans}
	}
	return
}

// Use selects the database on which queries are executed.
func (conn *Conn) Use(dbname string) error {
	return conn.withTimeout(func() error {
		return conn.destroyOnError(func() error {
			return conn.Conn.Use(dbname)
		})
	})
}

func (conn *Conn) prepareConnection() error {
	// set charset and collation if defined
	query := ""

	if len(conn.pool.config.Charset) > 0 {
		query = fmt.Sprintf("SET NAMES '%s'", conn.pool.config.Charset)
	}

	if len(conn.pool.config.Collation) > 0 {
		if len(query) > 0 {
			query = fmt.Sprintf("%s COLLATE '%s'", query, conn.pool.config.Collation)
		} else {
			return ErrCollationWithoutCharset
		}
	}

	if len(query) > 0 {
		if _, _, err := conn.Query(query); err != nil {
			return err
		}
	}

	return nil
}

// Is the connection suitable for use?
func (conn *Conn) verify() bool {
	if !conn.IsConnected() {
		conn.Destroy()
		return false
	}
	if conn.Ping() != nil {
		conn.Destroy()
		return false
	}
	if conn.pool.connectionExpiry > 0 && time.Now().After(conn.expiryDate) {
		conn.Destroy()
		return false
	}
	return true
}
