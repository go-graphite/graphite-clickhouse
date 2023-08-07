package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

// https://pkg.go.dev/crypto/tls#ClientAuthType
var supportedClientAuths = map[string]tls.ClientAuthType{
	"NoClientCert":               tls.NoClientCert,
	"RequestClientCert":          tls.RequestClientCert,
	"RequireAnyClientCert":       tls.RequireAnyClientCert,
	"VerifyClientCertIfGiven":    tls.VerifyClientCertIfGiven,
	"RequireAndVerifyClientCert": tls.RequireAndVerifyClientCert,
}

var supportedCurveIDs = map[string]tls.CurveID{
	"CurveP256": tls.CurveP256,
	"CurveP384": tls.CurveP384,
	"CurveP521": tls.CurveP521,
	"X25519":    tls.X25519,
}

type TLS struct {
	Certificates []CertificatePair `toml:"certificates"`
	CACertFiles  []string          `toml:"ca-cert"`
	ClientAuth   string            `toml:"client-auth"`

	ServerName         string   `toml:"server-name"`
	MinVersion         string   `toml:"min-version"` // supported formats: TLS10, TLS11, TLS12, TLS13
	MaxVersion         string   `toml:"max-version"` // supported formats: TLS10, TLS11, TLS12, TLS13
	InsecureSkipVerify bool     `toml:"insecure-skip-verify"`
	Curves             []string `toml:"curves"`
	CipherSuites       []string `toml:"cipher-suites"`
}

type CertificatePair struct {
	KeyFile  string `toml:"key"`
	CertFile string `toml:"cert"`
}

// ParseClientTLSConfig parses TLSConfig as it should be used for HTTPS client mTLS and returns &tls.Config, list of
// warnings or error if parsing has failed.
// At this moment warnings are only about insecure ciphers
func ParseClientTLSConfig(config *TLS) (*tls.Config, []string, error) {
	if len(config.Certificates) == 0 {
		return nil, nil, fmt.Errorf("no tls certificates provided")
	}

	caCertPool := x509.NewCertPool()
	for _, caCert := range config.CACertFiles {
		cert, err := os.ReadFile(caCert)
		if err != nil {
			return nil, nil, err
		}
		caCertPool.AppendCertsFromPEM(cert)
	}

	certificates := make([]tls.Certificate, 0, len(config.Certificates))
	for _, it := range config.Certificates {
		cert, err := tls.LoadX509KeyPair(it.CertFile, it.KeyFile)
		if err != nil {
			return nil, nil, err
		}
		certificates = append(certificates, cert)
	}

	minVersion, err := ParseTLSVersion(config.MinVersion)
	if err != nil {
		return nil, nil, err
	}
	maxVersion, err := ParseTLSVersion(config.MaxVersion)
	if err != nil {
		return nil, nil, err
	}
	curves, err := ParseCurves(config.Curves)
	if err != nil {
		return nil, nil, err
	}

	ciphers, warns, err := CipherSuitesToUint16(config.CipherSuites)
	if err != nil {
		return nil, warns, err
	}

	if config.InsecureSkipVerify {
		warns = append(warns, "InsecureSkipVerify is set to true, it's not recommended to use that in production")
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		Certificates:       certificates,
		ServerName:         config.ServerName,
		MinVersion:         minVersion,
		MaxVersion:         maxVersion,
		InsecureSkipVerify: config.InsecureSkipVerify,
		CipherSuites:       ciphers,
		CurvePreferences:   curves,
	}
	return tlsConfig, warns, nil
}

// ParseTLSVersion converts a TLS version string ("TLS10", "TLS11", "TLS12", "TLS13")
// to its respective uint16 constant.
// An empty string defaults to TLS version 1.3.
//
// Returns an error for unknown or invalid versions.
func ParseTLSVersion(version string) (uint16, error) {
	replacer := strings.NewReplacer("Version", "", ".", "", " ", "")
	version = replacer.Replace(version)
	switch version {
	case "TLS10":
		return tls.VersionTLS10, nil
	case "TLS11":
		return tls.VersionTLS11, nil
	case "TLS12":
		return tls.VersionTLS12, nil
	case "TLS13", "":
		return tls.VersionTLS13, nil
	default:
		return 0, errors.New("unknown TLS version")
	}
}

// ParseCurves returns list of tls.CurveIDs that can be passed to tls.Config or error if they are not supported
// ParseCurves also deduplicate input list
func ParseCurves(curveNames []string) ([]tls.CurveID, error) {
	inputCurveNamesMap := make(map[string]struct{})
	for _, name := range curveNames {
		inputCurveNamesMap[name] = struct{}{}
	}
	res := make([]tls.CurveID, 0, len(inputCurveNamesMap))
	for name := range inputCurveNamesMap {
		if curve, ok := supportedCurveIDs[name]; ok {
			res = append(res, curve)
		} else {
			return nil, fmt.Errorf("invalid curve name specified: %v", name)
		}
	}
	return res, nil
}

func ParseClientAuthType(clientAuth string) (tls.ClientAuthType, error) {
	if clientAuth == "" {
		return tls.NoClientCert, nil
	}
	if id, ok := supportedClientAuths[clientAuth]; ok {
		return id, nil
	} else {
		return tls.NoClientCert, fmt.Errorf("invalid auth type specified: %v", clientAuth)
	}
}

// CipherSuitesToUint16 for a given list of ciphers returns list of corresponding ids, list of insecure ciphers
// if cipher is unknown, it will return an error
func CipherSuitesToUint16(ciphers []string) ([]uint16, []string, error) {
	res := make([]uint16, 0)
	insecureCiphers := make([]string, 0)
	cipherList := tls.CipherSuites()

	cipherNames := make([]string, 0, len(cipherList))
	cipherSuites := make(map[string]uint16)

	for _, cipher := range cipherList {
		cipherSuites[cipher.Name] = cipher.ID
		if cipher.Insecure {
			insecureCiphers = append(insecureCiphers, cipher.Name)
		}
	}

	for _, c := range ciphers {
		if id, ok := cipherSuites[c]; ok {
			res = append(res, id)
		} else {
			return nil, nil, fmt.Errorf("unknown cipher specified: %v, supported ciphers: %+v", c, cipherNames)
		}
	}

	return res, insecureCiphers, nil
}
