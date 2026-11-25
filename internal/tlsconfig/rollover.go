package tlsconfig

import (
	"crypto/x509"
	"fmt"
	"time"
)

// RolloverLeaf describes one certificate chain and the identity and usages
// required of it. Include every active or retained rollback identity separately.
type RolloverLeaf struct {
	Name        string
	ChainPEM    []byte
	Identity    string
	Usages      []x509.ExtKeyUsage
	RetainFinal bool
}

// ValidateRollover checks certificate compatibility at the overlap and final
// trust stages. The caller must supply a complete inventory, including retained
// rollback fleets and their expected identities, and establish convergence and
// fresh handshakes separately. This function performs no rollout or mutation.
func ValidateRollover(overlapTrust, finalTrust []byte, leaves []RolloverLeaf, now, until time.Time) error {
	if now.IsZero() || !until.After(now) || len(leaves) == 0 {
		return fmt.Errorf("nonempty inventory and a future validity window are required")
	}
	overlap, err := parseTrust(overlapTrust)
	if err != nil {
		return fmt.Errorf("invalid overlap trust: %w", err)
	}
	final, err := parseTrust(finalTrust)
	if err != nil {
		return fmt.Errorf("invalid final trust: %w", err)
	}
	seen := make(map[string]bool, len(leaves))
	retained := false
	for _, leaf := range leaves {
		if leaf.Name == "" || seen[leaf.Name] || leaf.Identity == "" || len(leaf.Usages) == 0 {
			return fmt.Errorf("each leaf needs a unique name, identity and usages")
		}
		seen[leaf.Name] = true
		retained = retained || leaf.RetainFinal
		certs, err := parseCertificateChain(leaf.ChainPEM)
		if err != nil {
			return fmt.Errorf("leaf %q: %w", leaf.Name, err)
		}
		intermediates := certificatePool(certs[1:])
		for _, usage := range leaf.Usages {
			if usage != x509.ExtKeyUsageServerAuth && usage != x509.ExtKeyUsageClientAuth {
				return fmt.Errorf("leaf %q has an unsupported usage", leaf.Name)
			}
			options := x509.VerifyOptions{Roots: overlap, Intermediates: intermediates, DNSName: leaf.Identity, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{usage}}
			if err := verifyCertificateWindow(certs[0], options, until); err != nil {
				return fmt.Errorf("leaf %q fails overlap trust: %w", leaf.Name, err)
			}
			if leaf.RetainFinal {
				options.Roots = final
				if err := verifyCertificateWindow(certs[0], options, until); err != nil {
					return fmt.Errorf("leaf %q fails final trust: %w", leaf.Name, err)
				}
			}
		}
	}
	if !retained {
		return fmt.Errorf("at least one leaf must be retained under final trust")
	}
	return nil
}
