package pool

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/ziutek/mymysql/mysql"
	"io"
	"sync"
	"testing"
	"time"
)

const numConns = 5

var config = Config{
	Address:              "/var/run/mysqld/mysqld.sock",
	Protocol:             "unix",
	Username:             "testuser",
	Password:             "testpass",
	Database:             "test",
	MaxConnections:       numConns,
	MaxConnectionAge:     300,
	ConnectTimeout:       2,
	RequestTimeout:       5,
	KeepConnectionsAlive: true,
	Charset:              "",
	Collation:            "",
}

func getPool(t *testing.T, config Config) *Pool {
	pool, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestConnLifecycle(t *testing.T) {
	pool := getPool(t, config)
	conns := make([]*Conn, numConns)
	var err error

	// Create enough connections to fill the pool
	for i := 0; i < numConns; i++ {
		conns[i], err = pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conns[i])

		total, avail := pool.Size()
		assert.Equal(t, i+1, total, "Pool size should be %d", i+1)
		assert.Equal(t, 0, avail, "Number of available connections should be 0")
	}

	// Should produce an error because the pool is full
	_, err = pool.Get()
	assert.Error(t, err)

	// Release all the connections created earlier
	for i := 0; i < numConns; i++ {
		err := conns[i].Release()
		assert.NoError(t, err)

		total, avail := pool.Size()
		assert.Equal(t, numConns, total, "Pool size should be %d", numConns)
		assert.Equal(t, i+1, avail, "Number of available connections should be %d", i+1)
	}

	// Reserve and destroy connections
	for i := 0; i < numConns; i++ {
		conn, err := pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		total, avail := pool.Size()
		assert.Equal(t, numConns-i, total, "Pool size should be %d", numConns-i)
		assert.Equal(t, numConns-1-i, avail, "Number of available connections should be %d", numConns-1-i)

		conn.Destroy()

		total, avail = pool.Size()
		assert.Equal(t, numConns-1-i, total, "Pool size should be %d", numConns-1-i)
		assert.Equal(t, numConns-1-i, avail, "Number of available connections should be %d", numConns-1-i)
	}

	// Test concurrent reserve/release/destroy
	var wg sync.WaitGroup
	for i := 0; i < 8*numConns; i++ {
		wg.Add(1)
		go func(destroy bool) {
			conn, err := pool.Get()
			if assert.NoError(t, err) {
				assert.NotNil(t, conn)
				time.Sleep(time.Second / numConns)

				if destroy {
					conn.Destroy()
				} else {
					err = conn.Release()
					assert.NoError(t, err)
				}
			}

			wg.Done()
		}(i%2 == 1)
	}

	wg.Wait()
}

func TestConn_withTimeout(t *testing.T) {
	pool := getPool(t, config)
	var wg sync.WaitGroup

	for i := 0; i < 10; i += 2 {
		wg.Add(1)
		go func(i int) {
			conn, err := pool.Get()
			if assert.NoError(t, err) && assert.NotNil(t, conn) {
				err := conn.withTimeout(func() error {
					time.Sleep(time.Duration(i) * time.Second)
					return nil
				})
				if uint(i) < config.RequestTimeout {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
				conn.Release()
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestConn_destroyOnError(t *testing.T) {
	var testCases = map[error]bool{
		nil:                      false, // No error
		io.EOF:                   false,
		errors.New("oops"):       true,
		&mysql.Error{Code: 1021}: true,  // Disk full
		&mysql.Error{Code: 1022}: false, // Duplicate key
		&mysql.Error{Code: 1105}: false, // Unknown error
		&mysql.Error{Code: 1146}: false, // No such table
		&mysql.Error{Code: 1149}: false, // Syntax error
		&mysql.Error{Code: 1152}: true,  // Aborting connection
		&mysql.Error{Code: 1267}: false, // Illegal mix of collations
		&mysql.Error{Code: 2005}: true,  // Unknown host
		&mysql.Error{Code: 2006}: true,  // Server has gone away
		&mysql.Error{Code: 2056}: true,  // Lost connection
	}

	for errExpected, shouldDestroy := range testCases {
		pool := getPool(t, config)
		conn, err := pool.Get()
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		errReturned := conn.destroyOnError(func() error {
			return errExpected
		})

		assert.Equal(t, errExpected, errReturned)
		if shouldDestroy {
			assert.Nil(t, conn.pool, "Error: %s", errExpected)
		} else {
			assert.NotNil(t, conn.pool, "Error: %s", errExpected)
		}
	}
}
