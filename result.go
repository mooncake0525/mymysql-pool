package pool

import (
	"github.com/ziutek/mymysql/mysql"
)

// A Result is the result of a query executed on a connection in a database pool.
type Result struct {
	mysql.Result
	conn *Conn
}

// NextResult returns the next result set produced by a multi-statement query or
// stored procedure.
func (r *Result) NextResult() (result mysql.Result, err error) {
	r.conn.destroyOnError(func() error {
		result, err = r.Result.NextResult()
		return err
	})
	if result != nil {
		result = &Result{result, r.conn}
	}
	return
}

// GetRow returns the next row in the result set.
func (r *Result) GetRow() (row mysql.Row, err error) {
	r.conn.destroyOnError(func() error {
		row, err = r.Result.GetRow()
		return err
	})
	return
}

// GetRows returns all the rows in the result set.
func (r *Result) GetRows() (rows []mysql.Row, err error) {
	r.conn.destroyOnError(func() error {
		rows, err = r.Result.GetRows()
		return err
	})
	return
}

// GetFirstRow returns the first row in the result set.
func (r *Result) GetFirstRow() (row mysql.Row, err error) {
	r.conn.destroyOnError(func() error {
		row, err = r.Result.GetFirstRow()
		return err
	})
	return
}

// GetLastRow returns the last row in the result set.
func (r *Result) GetLastRow() (row mysql.Row, err error) {
	r.conn.destroyOnError(func() error {
		row, err = r.Result.GetLastRow()
		return err
	})
	return
}

// End discards all unread rows in the result.
func (r *Result) End() error {
	return r.conn.destroyOnError(r.Result.End)
}

// ScanRow reads a row directly from the network connection.
func (r *Result) ScanRow(row mysql.Row) error {
	return r.conn.destroyOnError(func() error {
		return r.Result.ScanRow(row)
	})
}
