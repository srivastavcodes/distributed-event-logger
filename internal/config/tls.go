package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSConfig struct {
	CertFile   string
	CAFile     string
	KeyFile    string
	Server     bool
	ServerAddr string
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	if cfg.CAFile != "" {
		if err := setupCA(tlsConfig, cfg); err != nil {
			return nil, err
		}
	}
	return tlsConfig, nil
}

func setupCA(tlsConfig *tls.Config, cfg TLSConfig) error {
	data, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return fmt.Errorf("failed to read CA file: %w", err)
	}
	ca := x509.NewCertPool()

	if !ca.AppendCertsFromPEM(data) {
		return fmt.Errorf("failed to parse root certificate: %q", cfg.CAFile)
	}
	if cfg.Server {
		tlsConfig.ClientCAs = ca
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.RootCAs = ca
		tlsConfig.ServerName = cfg.ServerAddr
	}
	return nil
}
