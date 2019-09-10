package pool

import (
	"github.com/ziutek/mymysql/mysql"
)

// A Stmt is a prepared statement associated with a connection in a database pool.
type Stmt struct {
	mysql.Stmt
	conn *Conn
	sql  string
}

// Delete destroys a prepared statement.
func (stmt *Stmt) Delete() error {
	return stmt.conn.destroyOnError(func() error {
		err := stmt.Stmt.Delete()
		if err == nil {
			delete(stmt.conn.statements, stmt.sql)
		}
		return err
	})
}

// Exec executes a prepared statement.
// The execution time is limited according to the pool's request timeout.
func (stmt *Stmt) Exec(params ...interface{}) (rows []mysql.Row, result mysql.Result, err error) {
	stmt.conn.withTimeout(func() error {
		return stmt.conn.destroyOnError(func() error {
			rows, result, err = stmt.Stmt.Exec(params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, stmt.conn}
	}
	return
}

// ExecFirst executes a prepared statement, returning only the first row in the
// result set.  The execution time is limited according to the pool's request
// timeout.
func (stmt *Stmt) ExecFirst(params ...interface{}) (row mysql.Row, result mysql.Result, err error) {
	stmt.conn.withTimeout(func() error {
		return stmt.conn.destroyOnError(func() error {
			row, result, err = stmt.Stmt.ExecFirst(params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, stmt.conn}
	}
	return
}

// ExecLast executes a prepared statement, returning only the last row in the
// result set.  The execution time is limited according to the pool's request
// timeout.
func (stmt *Stmt) ExecLast(params ...interface{}) (row mysql.Row, result mysql.Result, err error) {
	stmt.conn.withTimeout(func() error {
		return stmt.conn.destroyOnError(func() error {
			row, result, err = stmt.Stmt.ExecLast(params...)
			return err
		})
	})
	if err == nil {
		result = &Result{result, stmt.conn}
	}
	return
}

// String returns the SQL used to generate a prepared statement.
func (stmt *Stmt) String() string {
	return stmt.sql
}
