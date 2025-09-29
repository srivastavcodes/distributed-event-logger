package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig defines the parameters that SetupTLSConfig uses to determine
// what type of *tls.Config to return.
type TLSConfig struct {
	CAFile        string
	CertFile      string
	KeyFile       string
	Server        bool
	ServerAddress string
}

// SetupTLSConfig returns a configured *tls.Config ready for secure connections.
//
// This function sets up encryption settings based on whether you're running
// a server or connecting as a client:
//
// For servers (config.Server = true):
//   - Loads server certificate and private key for identity
//   - Sets up client certificate verification if CA file is provided
//   - Requires clients to present valid certificates
//
// For clients (config.Server = false):
//   - Loads client certificate and private key if needed
//   - Sets up server certificate verification using CA file
//   - Configures hostname verification via ServerName for SNI
func SetupTLSConfig(config TLSConfig) (*tls.Config, error) {
	var tlsConfig tls.Config
	if config.CertFile != "" && config.KeyFile != "" {
		var err error

		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if config.CAFile != "" {
		caPemCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(caPemCert)
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", CAFile)
		}
		if config.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
			// In TLS/SSL, ServerName is used for Server Name Indication (SNI). It specifies
			// the hostname that the client is attempting to connect to at the start of the
			// TLS handshake.
			tlsConfig.ServerName = config.ServerAddress
		}
	}
	return &tlsConfig, nil
}
