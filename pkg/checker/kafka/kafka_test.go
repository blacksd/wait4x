package kafka

import (
	"crypto/x509"
	"regexp"
	"testing"

	"github.com/atkrad/wait4x/v2/pkg/checker"
	"golang.org/x/net/context"
)

// TODO: produce before consume

func TestKafkaConsume(t *testing.T) {

	tests := map[string]struct {
		input checker.Checker
		want  error
	}{
		"consume_plain": {input: New("localhost:30100", "sample-topic", DefaultConnectionTimeout, DefaultNoVerify, DefaultUseTLS, x509.NewCertPool(), ":", regexp.MustCompile(".*"), "", false), want: nil},
		// consume_beginning
		// simple_auth
		// TLS_ignore
		// TLS_CA
		// message_content
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, _ := context.WithTimeout(context.Background(), DefaultConnectionTimeout)

			got := tc.input.Check(ctx)
			if tc.want != got {
				t.Fatalf("expected: %v, got: %v", tc.want, got)
			}
		})
	}
}
