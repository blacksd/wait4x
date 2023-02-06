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

package cmd

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/atkrad/wait4x/v2/pkg/checker/kafka"
	"github.com/atkrad/wait4x/v2/pkg/waiter"
	"github.com/spf13/cobra"
)

// NewKafkaCommand creates the kafka sub-command
func NewKafkaCommand() *cobra.Command {
	kafkaCommand := &cobra.Command{
		// TODO: do I need an extra command?
		Use:   "kafka bootstrap_servers topic [flags]",
		Short: "Check for a message in a Kafka topic",
		Args:  checkKafkaArgs,
		Example: `
  # Waiting for a new message in kafka topic 'my_topic' in PLAINTEXT and no auth. Use --connection-timeout to change the default of 10s.
  wait4x kafka '127.0.0.1:9092' 'my_topic'

  # Use TLS (use --no-verify to skip CA check) and basic auth 
  wait4x kafka '127.0.0.1:9092' 'my_topic' --tls --ca-cert ./ca.crt --basic-auth 'username:password'

  # Wait for a message that satisfies a specific regex
  wait4x kafka '127.0.0.1:9092' 'my_topic' --message-content '^.*Kubelet.*$' 

  # Join a specific Consumer Group, instead of creating a new one (use --from-beginning to restart from the first offset)
  wait4x kafka '127.0.0.1:9092' 'my_topic' --consumer-group 'my-cool-app-consumers' 
`,
		RunE: runKafkaE,
	}

	kafkaCommand.Flags().Duration("connection-timeout", kafka.DefaultConnectionTimeout, "Timeout is the maximum amount of time a dial will wait for a connection to complete.")

	// TODO: WARN --no-verify without --tls makes sense but it's odd
	kafkaCommand.Flags().Bool("tls", kafka.DefaultUseTLS, "TLS defines wether use SSL or PLAINTEXT.")
	kafkaCommand.Flags().Bool("no-verify", kafka.DefaultNoVerify, "NoVerify controls whether a client verifies the server's certificate chain and hostname.")
	// TODO: KO --cacert with --no-verify, pick on
	// TODO: KO --cacert with inaccessible path
	kafkaCommand.Flags().String("ca-cert", "", "CACert is the path to the root CA for validation.")

	// TODO: KO invalid --basic-auth
	kafkaCommand.Flags().String("basic-auth", kafka.DefaultBasicAuth, "BasicAuth defines a RFC2617-compliant Basic Authentication credential set.")

	// TODO: KO invalid --message-content
	kafkaCommand.Flags().String("message-content", kafka.DefaultMessageContent, "MessageContent is a valid regex to match a message to.")

	// TODO: KO invalid --consumer-group
	kafkaCommand.Flags().String("consumer-group", kafka.DefaultConsumerGroup, "ConsumerGroup defines a custom Consumer Group to create or join.")
	kafkaCommand.Flags().Bool("from-beginning", kafka.DefaultFromBeginning, "ConsumerGroup defines a custom Consumer Group to create or join.")

	return kafkaCommand
}

func checkKafkaArgs(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return errors.New("please specify both bootstrap server(s) and a topic to connect to")
	}
	// TODO: check if it's an IP and port
	for i, bs := range strings.Split(args[0], ",") {
		if _, _, err := net.SplitHostPort(bs); err != nil {
			return fmt.Errorf("failed to validate bootstrap server %v", i+1)
		}
	}
	return nil
}

func runKafkaE(cmd *cobra.Command, args []string) error {
	// TODO: not sure what to do with this
	invertCheck, _ := cmd.Flags().GetBool("invert-check")

	connectionTimeout, _ := cmd.Flags().GetDuration("connection-timeout")
	useTLS, _ := cmd.Flags().GetBool("tls")
	noVerify, _ := cmd.Flags().GetBool("no-verify")
	caCert, _ := cmd.Flags().GetString("ca-cert")
	basicAuth, _ := cmd.Flags().GetString("basic-auth")
	messageContent, _ := cmd.Flags().GetString("message-content")
	consumerGroup, _ := cmd.Flags().GetString("consumer-group")
	fromBeginning, _ := cmd.Flags().GetBool("from-beginning")

	messageContentRegExp, err := regexp.Compile(messageContent)
	if err != nil {
		return errors.New("the regex format is invalid")
	}

	// Build a *x509.CertPool from the path or nil
	caCertBundle := x509.NewCertPool()
	if caCert != "" {
		f, err := os.ReadFile(caCert)
		if err != nil {
			return errors.New("error reading CA cert file")
		}
		caCertBundle.AppendCertsFromPEM(f)
	}

	splitAuth := strings.SplitN(basicAuth, ":", 2)
	if len(splitAuth) != 2 {
		return errors.New("cannot build auth string")
	}

	//bootstrapServers

	kafkaChecker := kafka.New(
		args[0],
		args[1],
		connectionTimeout,
		useTLS,
		noVerify,
		caCertBundle,
		basicAuth,
		messageContentRegExp,
		consumerGroup,
		fromBeginning,
	)

	return waiter.WaitContext(
		cmd.Context(),
		kafkaChecker,
		waiter.WithInvertCheck(invertCheck),
		waiter.WithLogger(Logger))
}
