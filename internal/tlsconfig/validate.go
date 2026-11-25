// Package tlsconfig validates operator-supplied TLS material.
package tlsconfig

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"time"
)

// CertificateInfo contains public, non-secret certificate observations.
type CertificateInfo struct {
	Fingerprint string
	NotAfter    time.Time
}

// PublicMaterialDigests hashes only parsed public certificate material. The
// trust bundle is strictly checked before its bytes are exposed as a digest.
func PublicMaterialDigests(certificatePEM, trustPEM []byte) (string, string, error) {
	chain, err := parseCertificateChain(certificatePEM)
	if err != nil {
		return "", "", err
	}
	if _, err := parseTrust(trustPEM); err != nil {
		return "", "", err
	}
	leafDigest := sha256.Sum256(chain[0].Raw)
	trustDigest := sha256.Sum256(trustPEM)
	return hex.EncodeToString(leafDigest[:]), hex.EncodeToString(trustDigest[:]), nil
}

// Validate verifies a certificate/key pair, chain, usages and the identity used
// by its peer. The caller must check each identity and usage required by the
// selected provider and perform fresh handshakes after deployment.
func Validate(certificatePEM, keyPEM, trustPEM []byte, identity string, usages []x509.ExtKeyUsage, now time.Time) (CertificateInfo, error) {
	return ValidateWindow(certificatePEM, keyPEM, trustPEM, []string{identity}, usages, now, now)
}

// ValidateWindow requires a valid certificate path throughout the inclusive
// window for each supplied identity and usage. Successive paths can cover the
// window if their validity periods meet without a gap.
func ValidateWindow(certificatePEM, keyPEM, trustPEM []byte, identities []string, usages []x509.ExtKeyUsage, now, until time.Time) (CertificateInfo, error) {
	var result CertificateInfo
	if len(identities) == 0 || len(usages) == 0 || now.IsZero() || until.Before(now) {
		return result, fmt.Errorf("identities, usages and a valid observation window are required")
	}
	chain, err := parseCertificateChain(certificatePEM)
	if err != nil {
		return result, err
	}
	if _, err := tls.X509KeyPair(certificatePEM, keyPEM); err != nil {
		return result, fmt.Errorf("invalid certificate and key pair")
	}
	leaf := chain[0]
	roots, err := parseTrust(trustPEM)
	if err != nil {
		return result, err
	}
	intermediates := certificatePool(chain[1:])
	for _, identity := range identities {
		if identity == "" {
			return result, fmt.Errorf("certificate identity cannot be empty")
		}
		// Verify each usage independently. x509.Verify accepts any one of
		// multiple usages when they are supplied in one call.
		for _, usage := range usages {
			options := x509.VerifyOptions{Roots: roots, Intermediates: intermediates, DNSName: identity, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}}
			if err := verifyCertificateWindow(leaf, options, until); err != nil {
				return result, fmt.Errorf("certificate verification failed for %q: %w", identity, err)
			}
		}
	}
	fingerprint := sha256.Sum256(leaf.Raw)
	return CertificateInfo{Fingerprint: hex.EncodeToString(fingerprint[:]), NotAfter: leaf.NotAfter}, nil
}

func verifyCertificateWindow(leaf *x509.Certificate, options x509.VerifyOptions, until time.Time) error {
	for {
		chains, err := leaf.Verify(options)
		if err != nil {
			return fmt.Errorf("no valid chain at %s: %w", options.CurrentTime.Format(time.RFC3339Nano), err)
		}
		if options.CurrentTime.Equal(until) {
			return nil
		}
		var coveredUntil time.Time
		for _, chain := range chains {
			end := leaf.NotAfter
			for _, cert := range chain {
				if cert.NotAfter.Before(end) {
					end = cert.NotAfter
				}
			}
			if end.After(coveredUntil) {
				coveredUntil = end
			}
		}
		if !coveredUntil.Before(until) {
			return nil
		}
		// Verification includes NotAfter. Resume at the first instant not
		// covered by the paths that were valid at the last check.
		options.CurrentTime = coveredUntil.Add(time.Nanosecond)
	}
}

func certificatePool(certificates []*x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, certificate := range certificates {
		pool.AddCert(certificate)
	}
	return pool
}

func parseTrust(trustPEM []byte) (*x509.CertPool, error) {
	certificates, err := parseCertificateChain(trustPEM)
	if err != nil {
		return nil, fmt.Errorf("invalid trust bundle: %w", err)
	}
	for _, certificate := range certificates {
		if !certificate.IsCA {
			return nil, fmt.Errorf("trust bundle contains an invalid CA")
		}
	}
	return certificatePool(certificates), nil
}

func parseCertificateChain(data []byte) ([]*x509.Certificate, error) {
	const begin = "-----BEGIN CERTIFICATE-----"
	var chain []*x509.Certificate
	for len(data) > 0 {
		next := bytes.TrimLeft(data, " \t\r\n")
		if len(next) == 0 {
			break
		}
		// Keep BEGIN at the start of its line, as required by the consumers
		// that load the original, unmodified PEM.
		skipped := len(data) - len(next)
		if skipped > 0 && data[skipped-1] != '\n' {
			return nil, fmt.Errorf("invalid certificate PEM")
		}
		data = next
		if !bytes.HasPrefix(data, []byte(begin)) {
			return nil, fmt.Errorf("invalid certificate PEM")
		}
		block, rest := pem.Decode(data)
		// Decode can skip an invalid block and return a later one.
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 ||
			bytes.Contains(data[len(begin):len(data)-len(rest)], []byte("-----BEGIN ")) {
			return nil, fmt.Errorf("invalid certificate PEM")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("invalid certificate in PEM")
		}
		chain = append(chain, certificate)
		data = rest
	}
	if len(chain) == 0 {
		return nil, fmt.Errorf("empty certificate PEM")
	}
	return chain, nil
}
