// Command peer-tls-probe checks a fresh TLS 1.2 handshake on Java peer transport.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("peer-tls-probe", flag.ContinueOnError)
	address := flags.String("address", "", "target peer host or IP and transport port")
	identity := flags.String("server-name", "", "target Pod DNS identity")
	pin := flags.String("server-leaf-sha256", "", "expected target leaf certificate SHA-256 in hex")
	clientPin := flags.String("client-leaf-sha256", "", "expected mounted client leaf certificate SHA-256 in hex")
	noClient := flags.Bool("without-client-certificate", false, "attempt a handshake without presenting a certificate")
	certificate := flags.String("certificate", "/var/run/serverless/peer-cert/tls.crt", "client certificate chain")
	key := flags.String("key", "/var/run/serverless/peer-cert/tls.key", "client private key")
	trust := flags.String("trust", "/var/run/serverless/peer-trust/ca.crt", "peer CA bundle")
	timeout := flags.Duration("timeout", 5*time.Second, "handshake timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	expected, err := hex.DecodeString(*pin)
	if err != nil || len(expected) != sha256.Size || *address == "" || *identity == "" || *timeout <= 0 || flags.NArg() != 0 {
		return fmt.Errorf("address, server name, SHA-256 pin and positive timeout are required")
	}
	if *noClient && *clientPin != "" {
		return fmt.Errorf("a client leaf pin requires a client certificate")
	}
	rootsPEM, err := os.ReadFile(*trust)
	if err != nil {
		return fmt.Errorf("read peer trust bundle: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return fmt.Errorf("peer trust bundle contains no usable certificates")
	}
	var pair tls.Certificate
	var clientFingerprint [sha256.Size]byte
	if !*noClient {
		pair, err = tls.LoadX509KeyPair(*certificate, *key)
		if err != nil {
			return fmt.Errorf("load peer client certificate and key: %w", err)
		}
		clientFingerprint = sha256.Sum256(pair.Certificate[0])
		if *clientPin != "" {
			pinned, err := hex.DecodeString(*clientPin)
			if err != nil || !bytes.Equal(pinned, clientFingerprint[:]) {
				return fmt.Errorf("mounted client peer leaf SHA-256 was %x", clientFingerprint)
			}
		}
	}
	clientRequested := false
	config := &tls.Config{
		RootCAs: roots, ServerName: *identity,
		MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			clientRequested = true
			return &pair, nil
		},
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("target presented no peer certificate")
			}
			actual := sha256.Sum256(state.PeerCertificates[0].Raw)
			if !bytes.Equal(actual[:], expected) {
				return fmt.Errorf("target peer leaf SHA-256 was %x", actual)
			}
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	dialer := &tls.Dialer{NetDialer: &net.Dialer{}, Config: config}
	conn, err := dialer.DialContext(ctx, "tcp", *address)
	if err != nil {
		return fmt.Errorf("peer mTLS handshake with %s: %w", *identity, err)
	}
	defer conn.Close()
	if *noClient {
		return fmt.Errorf("target accepted a handshake without a client certificate")
	}
	if !clientRequested {
		return fmt.Errorf("target did not request a client certificate")
	}
	_, err = fmt.Fprintf(output, "%s served peer leaf SHA-256 %x over TLS 1.2 with client leaf SHA-256 %x\n", *identity, expected, clientFingerprint)
	return err
}
