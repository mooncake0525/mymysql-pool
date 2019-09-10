package pool

import (
	"github.com/ziutek/mymysql/mysql"
)

// A Transaction is provides a means of executing multiple statements as a
// single, atomic operation.
type Transaction struct {
	*Conn
	trans mysql.Transaction
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	return t.Conn.withTimeout(func() error {
		return t.Conn.destroyOnError(func() error {
			return t.trans.Commit()
		})
	})
}

// Rollback rolls back the transaction.
func (t *Transaction) Rollback() error {
	return t.Conn.withTimeout(func() error {
		return t.Conn.destroyOnError(func() error {
			return t.trans.Rollback()
		})
	})
}

// Do binds a statement to the transaction.
func (t *Transaction) Do(stmt mysql.Stmt) mysql.Stmt {
	return t.trans.Do(stmt)
}

// IsValid returns true if the transaction is connected to an open connection.
func (t *Transaction) IsValid() bool {
	return t.trans.IsValid()
}
