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
	"errors"

	"github.com/atkrad/wait4x/v2/pkg/checker"
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

  # Wait for a message that satisfies a specific regex (use --from-beginning to start from the first offset)
  wait4x kafka '127.0.0.1:9092' 'my_topic' --message-content '^.*Kubelet.*$' 

  # Join a specific Consumer Group, instead of creating a new one
  wait4x kafka '127.0.0.1:9092' 'my_topic' --consumer-group 'my-cool-app-consumers' 
`,
		RunE: runKafkaE,
	}

	kafkaCommand.Flags().Duration("connection-timeout", kafka.DefaultConnectionTimeout, "Timeout is the maximum amount of time a dial will wait for a connection to complete.")

	// TODO: WARN --no-verify without --tls makes sense but it's odd
	kafkaCommand.Flags().Bool("tls", kafka.DefaultTLS, "TLS defines wether use SSL or PLAINTEXT.")
	kafkaCommand.Flags().Bool("no-verify", kafka.DefaultNoVerify, "NoVerify controls whether a client verifies the server's certificate chain and hostname.")
	// TODO: KO --cacert with --no-verify, pick on
	// TODO: KO --cacert with inaccessible path
	kafkaCommand.Flags().String("ca-cert", kafka.DefaultCACert, "CACert is the path to the root CA for validation.")

	// TODO: KO invalid --basic-auth
	kafkaCommand.Flags().String("basic-auth", kafka.DefaultBasicAuth, "BasicAuth defines a RFC2617-compliant Basic Authentication credential set.")

	// TODO: KO invalid --message-content
	kafkaCommand.Flags().String("message-content", kafka.DefaultMessageContent, "MessageContent is a valid regex to match a message to.")

	// TODO: KO invalid --consumer-group
	kafkaCommand.Flags().String("consumer-group", kafka.DefaultConsumerGroup, "ConsumerGroup defines a custom Consumer Group to create or join.")

	return kafkaCommand
}

func checkKafkaArgs(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return errors.New("Please specify both bootstrap server(s) and a topic to connect to.")
	}
	return nil
}

func runKafkaE(cmd *cobra.Command, args []string) error {
	interval, _ := cmd.Flags().GetDuration("interval")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	invertCheck, _ := cmd.Flags().GetBool("invert-check")

	conTimeout, _ := cmd.Flags().GetDuration("connection-timeout")
	NoVerify, _ := cmd.Flags().GetBool("no-verify")

	// ArgsLenAtDash returns -1 when -- was not specified
	if i := cmd.ArgsLenAtDash(); i != -1 {
		args = args[:i]
	} else {
		args = args[:]
	}

	checkers := make([]checker.Checker, 0)
	for _, arg := range args {
		rc := kafka.New(
			arg,
			kafka.WithTimeout(conTimeout),
			kafka.WithNoVerify(NoVerify),
		)

		checkers = append(checkers, rc)
	}

	return waiter.WaitParallelContext(
		cmd.Context(),
		checkers,
		waiter.WithTimeout(timeout),
		waiter.WithInterval(interval),
		waiter.WithInvertCheck(invertCheck),
		waiter.WithLogger(Logger),
	)
}
