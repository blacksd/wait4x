// Copyright 2023 Marco Bulgarini
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/atkrad/wait4x/v2/pkg/checker"
	"github.com/streadway/amqp"
)

// Option configures a Kafka.
type Option func(r *Kafka)

const (
	// FIXME: Kafka is heartless and polyglot
	// DefaultHeartbeat is the default heartbeat duration
	DefaultHeartbeat = 10 * time.Second
	DefaultLocale    = "en_US"
	// DefaultNoVerify is the default insecure skip tls verify

	// DefaultConnectionTimeout is the default connection timeout duration
	DefaultConnectionTimeout = 3 * time.Second
	// DefaultLocale is the default connection locale
	DefaultNoVerify = false
	// DefaultTLS defines wether use SSL or PLAINTEXT
	DefaultTLS = false
	// DefaultCACert is the default path for the certificate to verify TLS
	DefaultCACert = "./ca.crt"
	// TODO: describe
	DefaultBasicAuth = ":"
	// TODO: describe
	DefaultMessageContent = ".*"
)

// TODO: describe
var DefaultConsumerGroup string = fmt.Sprintf("wait4x-%s", strings.Split(uuid.New().String(), "-")[0])

// Kafka represents a Kafka checker
type Kafka struct {
	dsn      string
	timeout  time.Duration
	NoVerify bool
}

// New creates the Kafka checker
func New(dsn string, opts ...Option) checker.Checker {
	t := &Kafka{
		dsn:      dsn,
		timeout:  DefaultConnectionTimeout,
		NoVerify: DefaultNoVerify,
	}

	// apply the list of options to Kafka
	for _, opt := range opts {
		opt(t)
	}

	return t
}

// WithTimeout configures a timeout for maximum amount of time a dial will wait for a connection to complete
func WithTimeout(timeout time.Duration) Option {
	return func(r *Kafka) {
		r.timeout = timeout
	}
}

// WithNoVerify controls whether a client verifies the server's certificate chain and hostname
func WithNoVerify(NoVerify bool) Option {
	return func(r *Kafka) {
		r.NoVerify = NoVerify
	}
}

// Identity returns the identity of the checker
func (r Kafka) Identity() (string, error) {
	u, err := amqp.ParseURI(r.dsn)
	if err != nil {
		return "", fmt.Errorf("can't retrieve the checker identity: %w", err)
	}

	return fmt.Sprintf("%s:%d", u.Host, u.Port), nil
}

// Check checks Kafka connection
func (r *Kafka) Check(ctx context.Context) (err error) {
	conn, err := amqp.DialConfig(
		r.dsn,
		amqp.Config{
			Heartbeat: DefaultHeartbeat,
			Locale:    DefaultLocale,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: r.NoVerify,
			},
			Dial: func(network, addr string) (net.Conn, error) {
				d := net.Dialer{Timeout: r.timeout}
				conn, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}

				// Heartbeating hasn't started yet, don't stall forever on a dead server.
				// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
				// the deadline is cleared in openComplete.
				if err := conn.SetDeadline(time.Now().Add(r.timeout)); err != nil {
					return nil, err
				}

				return conn, nil
			},
		},
	)

	if err != nil {
		if checker.IsConnectionRefused(err) {
			return checker.NewExpectedError(
				"failed to establish a connection to the Kafka server", err,
				"dsn", r.dsn,
			)
		}

		return err
	}

	defer func(conn *amqp.Connection) {
		if connerr := conn.Close(); connerr != nil {
			err = connerr
		}
	}(conn)

	_, err = conn.Channel()
	if err != nil {
		return err
	}

	return nil
}
