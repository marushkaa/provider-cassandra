/*
Copyright 2021 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cassandra

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
)

type DB interface {
	// Exec executes a CQL statement.
	Exec(ctx context.Context, query string, args ...interface{}) error

	// Query performs a query and returns an iterator for the results.
	Query(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error)

	// scans
	Scan(iter *gocql.Iter, dest ...interface{}) bool

	// Close closes the Cassandra session.
	Close()

	// GetConnectionDetails returns the connection details for a user of this DB.
	GetConnectionDetails(username, password string) managed.ConnectionDetails
}

type CassandraDB struct {
	session  *gocql.Session
	endpoint string
	port     string
}

// New initializes a new Cassandra client.
func New(creds map[string][]byte, keyspace string, consistencyLevel string) DB {
	endpoint := string(creds[xpv1.ResourceCredentialsSecretEndpointKey])
	port := string(creds[xpv1.ResourceCredentialsSecretPortKey])

	// Combine endpoint and port
	host := endpoint
	if port != "" {
		host = fmt.Sprintf("%s:%s", endpoint, port)
	}

	cluster := gocql.NewCluster(host)

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: string(creds[xpv1.ResourceCredentialsSecretUserKey]),
		Password: string(creds[xpv1.ResourceCredentialsSecretPasswordKey]),
	}

	if keyspace != "" {
		cluster.Keyspace = keyspace
	}

	cluster.Consistency = parseConsistencyLevel(consistencyLevel)
	session, _ := cluster.CreateSession()

	return CassandraDB{
		session:  session,
		endpoint: endpoint,
		port:     port,
	}
}

func parseConsistencyLevel(level string) gocql.Consistency {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "ANY":
		return gocql.Any
	case "ONE":
		return gocql.One
	case "TWO":
		return gocql.Two
	case "THREE":
		return gocql.Three
	case "QUORUM":
		return gocql.Quorum
	case "ALL":
		return gocql.All
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	case "EACH_QUORUM":
		return gocql.EachQuorum
	case "LOCAL_ONE":
		return gocql.LocalOne
	default:
		return gocql.All
	}
}

// Exec executes a CQL statement and returns an error if the session is not available or the execution fails.
func (c CassandraDB) Exec(ctx context.Context, query string, args ...interface{}) error {
	if c.session == nil {
		return errors.New("Cassandra session is not initialized")
	}

	err := c.session.Query(query, args...).WithContext(ctx).Exec()
	if err != nil {
		return errors.New("failed to execute query: " + err.Error())
	}

	return nil
}

// Query performs a query and returns an iterator for the results or an error if the session is not available.
func (c CassandraDB) Query(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
	if c.session == nil {
		return nil, errors.New("cassandra session is not initialized")
	}

	iter := c.session.Query(query, args...).WithContext(ctx).Iter()
	if iter == nil {
		return nil, errors.New("failed to execute query or no iterator returned")
	}

	return iter, nil
}

// Query performs scan on a iter
func (c CassandraDB) Scan(iter *gocql.Iter, dest ...interface{}) bool {
	return iter.Scan(dest...)
}

// Close closes the Cassandra session.
func (c CassandraDB) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

// GetConnectionDetails returns the connection details for a user of this DB.
func (c CassandraDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	return managed.ConnectionDetails{
		xpv1.ResourceCredentialsSecretUserKey:     []byte(username),
		xpv1.ResourceCredentialsSecretPasswordKey: []byte(password),
		xpv1.ResourceCredentialsSecretEndpointKey: []byte(c.endpoint),
		xpv1.ResourceCredentialsSecretPortKey:     []byte(c.port),
	}
}

// QuoteIdentifier safely quotes an identifier to prevent SQL injection.
// Cassandra uses double quotes to delimit identifiers.
func QuoteIdentifier(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}
