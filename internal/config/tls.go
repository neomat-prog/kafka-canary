package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"software.sslmate.com/src/go-pkcs12"
)

// BuildTLSConfig builds an mTLS config from Strimzi PKCS12 keystores (.p12),
// matching the Java client env (KAFKA_SSL_*). Returns (nil, nil) when nothing is
// configured (plaintext local dev).
//
//	CACertPath         <- KAFKA_SSL_TRUSTSTORE_LOCATION  (ca.p12)
//	TruststorePassword <- KAFKA_SSL_TRUSTSTORE_PASSWORD  (ca.password)
//	ClientCertPath     <- KAFKA_SSL_KEYSTORE_LOCATION    (user.p12)
//	KeystorePassword   <- KAFKA_SSL_KEYSTORE_PASSWORD    (user.password)
func BuildTLSConfig(cfg Config) (*tls.Config, error) {
	if cfg.CACertPath == "" && cfg.ClientCertPath == "" {
		return nil, nil
	}

	truststoreData, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("reading truststore: %w", err)
	}
	caCerts, err := pkcs12.DecodeTrustStore(truststoreData, cfg.TruststorePassword)
	if err != nil {
		return nil, fmt.Errorf("decoding truststore (wrong password or corrupt file): %w", err)
	}
	caPool := x509.NewCertPool()
	for _, cert := range caCerts {
		caPool.AddCert(cert)
	}

	keystoreData, err := os.ReadFile(cfg.ClientCertPath)
	if err != nil {
		return nil, fmt.Errorf("reading keystore: %w", err)
	}
	privKey, leaf, caChain, err := pkcs12.DecodeChain(keystoreData, cfg.KeystorePassword)
	if err != nil {
		return nil, fmt.Errorf("decoding keystore: %w", err)
	}
	clientCert := tls.Certificate{
		Certificate: [][]byte{leaf.Raw},
		PrivateKey:  privKey,
		Leaf:        leaf,
	}
	for _, c := range caChain {
		clientCert.Certificate = append(clientCert.Certificate, c.Raw)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
