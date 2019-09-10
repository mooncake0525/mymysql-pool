// Package pool implements a simple connection pool for the MyMySQL driver
package pool

import (
	"fmt"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Use the native driver
	"sync"
	"time"
)

// A Pool is a set of one or more persistent database connections.
type Pool struct {
	openConnections  map[*Conn]struct{}
	idleConnections  chan *Conn
	numPending       uint
	mutex            *sync.Mutex
	config           Config
	connectionExpiry time.Duration
	connectTimeout   time.Duration
	requestTimeout   time.Duration
}

// Config packs all the configuration options for a pool in a simple, easy-to-use container.
type Config struct {
	Address              string
	Protocol             string
	Username             string
	Password             string
	Database             string
	MaxConnections       uint
	MaxConnectionAge     uint
	ConnectTimeout       uint
	RequestTimeout       uint
	KeepConnectionsAlive bool
	Charset              string
	Collation            string
}

// New initializes a connection pool.
func New(config Config) (*Pool, error) {
	pool := &Pool{
		openConnections:  make(map[*Conn]struct{}),
		idleConnections:  make(chan *Conn, config.MaxConnections),
		mutex:            new(sync.Mutex),
		config:           config,
		connectionExpiry: time.Duration(config.MaxConnectionAge) * time.Second,
		connectTimeout:   time.Duration(config.ConnectTimeout) * time.Second,
		requestTimeout:   time.Duration(config.RequestTimeout) * time.Second,
	}
	return pool, nil
}

// Size returns the total number of connections managed by the pool and the
// number of those that are currently available.
func (pool *Pool) Size() (total, available int) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	return len(pool.openConnections), len(pool.idleConnections)
}

// Ping sends a simple query to the database to determine its status.
func (pool *Pool) Ping() (time.Duration, error) {
	conn, err := pool.Get()
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	start := time.Now()
	_, _, err = conn.Query("SELECT 1")
	return time.Since(start), err
}

// Conn initializes and returns a new connection.
func (pool *Pool) Conn() (*Conn, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	return pool.createConn()
}

// Assumes that the pool is already locked
func (pool *Pool) createConn() (*Conn, error) {
	conn := &Conn{
		mysql.New(
			pool.config.Protocol,
			"",
			pool.config.Address,
			pool.config.Username,
			pool.config.Password,
			pool.config.Database,
		),
		pool,
		map[string]*Stmt{},
		time.Now().Add(pool.connectionExpiry),
	}

	conn.Conn.SetTimeout(pool.connectTimeout)
	err := conn.Connect()
	if err == nil {
		pool.openConnections[conn] = struct{}{}
		return conn, nil
	}
	return nil, err
}

// Get retrieves a database connection from the pool.
func (pool *Pool) Get() (*Conn, error) {
	for {
		select {

		// If a connection is available immediately, use that
		case conn := <-pool.idleConnections:
			if conn.verify() {
				return conn, nil
			}

		default:

			// Create a new connection if we're still below the maximum
			pool.mutex.Lock()
			if len(pool.openConnections) < int(pool.config.MaxConnections) {
				conn, err := pool.createConn()
				pool.mutex.Unlock()
				return conn, err
			}

			pool.numPending++
			pool.mutex.Unlock()
			defer func() {
				pool.mutex.Lock()
				pool.numPending--
				pool.mutex.Unlock()
			}()

			// Otherwise wait for a connection to become available
			select {
			case conn := <-pool.idleConnections:
				if conn.verify() {
					return conn, nil
				}

			case <-time.After(pool.connectTimeout):
				total, avail := pool.Size()
				return nil, fmt.Errorf("Timeout reached while waiting for SQL connection (total: %d, avail: %d, max: %d)", total, avail, pool.config.MaxConnections)
			}
		}
	}
}
