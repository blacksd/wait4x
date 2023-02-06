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
	"crypto/x509"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/atkrad/wait4x/v2/pkg/checker"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
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
	DefaultConnectionTimeout = 15 * time.Second
	// DefaultLocale is the default connection locale
	DefaultNoVerify = false
	// DefaultTLS defines wether use SSL or PLAINTEXT
	DefaultUseTLS = false
	// DefaultCACert is the default path for the certificate to verify TLS
	// TODO: describe
	DefaultBasicAuth = ":"
	// TODO: describe
	DefaultMessageContent = ".*"
	// TODO: describe
	DefaultFromBeginning = false
)

// TODO: describe
var DefaultConsumerGroup string = fmt.Sprintf("wait4x-%s", strings.Split(uuid.New().String(), "-")[0])

// Kafka represents a Kafka checker
type Kafka struct {
	bootstrapServers string
	topic            string
	timeout          time.Duration
	noVerify         bool
	useTLS           bool
	caCert           *x509.CertPool
	basicAuth        string
	messageContent   *regexp.Regexp
	consumerGroup    string
	fromBeginning    bool
}

// New creates the Kafka checker
func New(bootstrapServers string,
	topic string, timeout time.Duration,
	noVerify bool,
	useTLS bool,
	caCert *x509.CertPool,
	basicAuth string,
	messageContent *regexp.Regexp,
	consumerGroup string,
	fromBeginning bool) checker.Checker {
	c := &Kafka{
		bootstrapServers: bootstrapServers,
		topic:            topic,
		timeout:          timeout,
		noVerify:         noVerify,
		useTLS:           useTLS,
		caCert:           caCert,
		basicAuth:        basicAuth,
		messageContent:   messageContent,
		consumerGroup:    consumerGroup,
		fromBeginning:    fromBeginning,
	}

	return c
}

// Identity returns the identity of the checker
func (r Kafka) Identity() (string, error) {
	/* 	u, err := amqp.ParseURI(r.dsn)
	   	if err != nil {
	   		return "", fmt.Errorf("can't retrieve the checker identity: %w", err)
	   	}

	   	return fmt.Sprintf("%s:%d", u.Host, u.Port), nil */
	// return "eh", nil
	return fmt.Sprintf("%s/%s", r.bootstrapServers, r.topic), nil
}

// Check checks Kafka connection
func (r *Kafka) Check(ctx context.Context) (err error) {
	d := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
		SASLMechanism: func() sasl.Mechanism {
			if r.basicAuth == ":" {
				return nil
			} else {
				splitAuth := strings.SplitN(r.basicAuth, ":", 2)
				return plain.Mechanism{
					Username: splitAuth[0],
					Password: splitAuth[1],
				}
			}
		}(),
		TLS: func() *tls.Config {
			if r.useTLS {
				return &tls.Config{
					InsecureSkipVerify: r.noVerify,
					RootCAs:            r.caCert,
				}
			} else {
				return nil
			}
		}(),
	}
	// conn, err := kafka.DialLeader(ctx, "tcp", r.bootstrapServers, r.topic, partition)
	k := kafka.NewReader(kafka.ReaderConfig{
		// TODO: if more than one bootstrapServer is given, make it a slice
		Brokers: []string{r.bootstrapServers},
		Topic:   r.topic,
		GroupID: r.consumerGroup,
		Dialer:  d,
		StartOffset: func() int64 {
			if r.fromBeginning {
				return kafka.FirstOffset
			} else {
				return kafka.LastOffset
			}
		}(),

		MinBytes: 10,   // 1KB
		MaxBytes: 10e6, // 10MB

	})
	defer k.Close()
	for {
		// TODO: check for context timeout
		m, err := k.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				return checker.NewExpectedError(
					"timeout trying to connect to partition leader", err,
					"bootstrap", r.bootstrapServers,
				)
			}
			break
		}
		if r.messageContent.Match(m.Value) {
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s checks value!\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	/* 	conn, err := amqp.DialConfig(
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
	*/
	return nil
}
