package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	ModeSocks5TLS = "socks5tls"
	ModeHTTPS     = "https"

	minConnectTimeoutMS          = 1000
	maxConnectTimeoutMS          = 120000
	minIdleTimeoutMS             = 5000
	maxIdleTimeoutMS             = 3600000
	defaultUDPIdleTimeoutMS      = 5000
	minRuntimeBufferBytes        = 4096
	maxRuntimeBufferBytes        = 4 * 1024 * 1024
	maxDialConcurrency           = 1024
	maxHandlerConcurrency        = 4096
	maxDNSCacheEntries           = 65536
	maxDNSOverTCPExtraBytes      = 65535 + 2
	maxActiveRelayBufferEstimate = 1 * 1024 * 1024 * 1024
)

/* Internal runtime configuration assembled from the compact flat JSON payload. */
type Config struct {
	Mode      string
	AutoRoute bool
	Tun       TunConfig
	Upstream  UpstreamConfig
	Upstreams []UpstreamConfig
	Runtime   RuntimeConfig
	Routing   RoutingConfig
}

/* Local tunnel interface settings. */
type TunConfig struct {
	Name string
	MTU  int
	IPv4 string
	IPv6 string
}

/* Remote proxy and TLS credentials. */
type UpstreamConfig struct {
	Addr               string
	ServerName         string
	CAPEM              string
	ClientPEM          string
	ClientKey          string
	InsecureSkipVerify bool
	Username           string
	Password           string
}

/* Controls timeout and buffer behavior. */
type RuntimeConfig struct {
	ConnectTimeoutMS      int
	IdleTimeoutMS         int
	UDPIdleTimeoutMS      int
	TCPBuffer             int
	UDPBuffer             int
	EnableUDP             bool
	TCPDialConcurrency    int
	UDPDialConcurrency    int
	TCPHandlerConcurrency int
	UDPHandlerConcurrency int
	DNSCacheSize          int
}

/* Controls direct/proxy split routing decisions inside the TUN engine. */
type RoutingConfig struct {
	Enabled         bool
	EnabledSet      bool
	DefaultOutbound string
	DirectCIDRs     []string
	ProxyCIDRs      []string
	DirectCIDRFiles []string
	ProxyCIDRFiles  []string
}

func (u UpstreamConfig) trimmedClientCertificatePaths() (string, string) {
	return strings.TrimSpace(u.ClientPEM), strings.TrimSpace(u.ClientKey)
}

func (u UpstreamConfig) hasBasicAuthCredentials() bool {
	return strings.TrimSpace(u.Username) != "" || strings.TrimSpace(u.Password) != ""
}

func (c *Config) UpstreamAddrsString() string {
	if c == nil {
		return ""
	}
	addrs := make([]string, 0, len(c.Upstreams))
	for _, upstream := range c.Upstreams {
		if addr := strings.TrimSpace(upstream.Addr); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return strings.Join(addrs, ",")
}

type configPayload struct {
	Mode      string `json:"mode"`
	AutoRoute *bool  `json:"auto_route"`

	Tun     string `json:"tun"`
	TunMTU  int    `json:"tun_mtu"`
	TunIPv4 string `json:"tun_ipv4"`
	TunIPv6 string `json:"tun_ipv6"`

	Upstream  string            `json:"upstream"`
	Upstreams []upstreamPayload `json:"upstreams"`

	ClientPEM          string `json:"client_pem"`
	ClientKey          string `json:"client_key"`
	CAPEM              string `json:"ca_pem"`
	InsecureSkipVerify *bool  `json:"insecure_skip_verify"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	HTTPBasicUser      string `json:"http_basic_user"`
	HTTPBasicPass      string `json:"http_basic_pass"`

	ConnectTimeoutMS   int   `json:"connect_timeout_ms"`
	IdleTimeoutMS      int   `json:"idle_timeout_ms"`
	UDPIdleTimeoutMS   int   `json:"udp_idle_timeout_ms"`
	EnableUDP          *bool `json:"enable_udp"`
	TCPDialConcurrency int   `json:"tcp_dial_concurrency"`
	UDPDialConcurrency int   `json:"udp_dial_concurrency"`
	DNSCacheSize       int   `json:"dns_cache_size"`

	Routing routingPayload `json:"routing"`
}

type routingPayload struct {
	Enabled         *bool    `json:"enabled"`
	DefaultOutbound string   `json:"default"`
	DirectCIDRs     []string `json:"direct_cidrs"`
	ProxyCIDRs      []string `json:"proxy_cidrs"`
	DirectCIDRFiles []string `json:"direct_cidr_files"`
	ProxyCIDRFiles  []string `json:"proxy_cidr_files"`
}

type upstreamPayload struct {
	Addr       string  `json:"addr"`
	ServerName string  `json:"server_name"`
	CAPEM      *string `json:"ca_pem"`
	Username   *string `json:"username"`
	Password   *string `json:"password"`
}

/* Reads, decodes, defaults, and validates the config file. */
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	cfg, err := decodeConfig(data)
	if err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

/* Parses compact JSON fields into the runtime configuration model. */
func decodeConfig(data []byte) (*Config, error) {
	if err := validateConfigJSONShape(data); err != nil {
		return nil, err
	}
	var p configPayload
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("config must contain exactly one JSON object")
		}
		return nil, fmt.Errorf("trailing config data: %w", err)
	}
	cfg := &Config{
		Mode:      p.Mode,
		AutoRoute: true,
		Tun: TunConfig{
			Name: strings.TrimSpace(p.Tun),
			MTU:  p.TunMTU,
			IPv4: strings.TrimSpace(p.TunIPv4),
			IPv6: strings.TrimSpace(p.TunIPv6),
		},
		Upstream: UpstreamConfig{
			Addr:               strings.TrimSpace(p.Upstream),
			CAPEM:              strings.TrimSpace(p.CAPEM),
			ClientPEM:          strings.TrimSpace(p.ClientPEM),
			ClientKey:          strings.TrimSpace(p.ClientKey),
			Username:           strings.TrimSpace(p.Username),
			Password:           strings.TrimSpace(p.Password),
			InsecureSkipVerify: false,
		},
		Runtime: RuntimeConfig{
			ConnectTimeoutMS:   p.ConnectTimeoutMS,
			IdleTimeoutMS:      p.IdleTimeoutMS,
			UDPIdleTimeoutMS:   p.UDPIdleTimeoutMS,
			EnableUDP:          false,
			TCPDialConcurrency: p.TCPDialConcurrency,
			UDPDialConcurrency: p.UDPDialConcurrency,
			DNSCacheSize:       p.DNSCacheSize,
		},
		Routing: RoutingConfig{
			DefaultOutbound: strings.TrimSpace(p.Routing.DefaultOutbound),
			DirectCIDRs:     trimStringSlice(p.Routing.DirectCIDRs),
			ProxyCIDRs:      trimStringSlice(p.Routing.ProxyCIDRs),
			DirectCIDRFiles: trimStringSlice(p.Routing.DirectCIDRFiles),
			ProxyCIDRFiles:  trimStringSlice(p.Routing.ProxyCIDRFiles),
		},
	}

	if p.InsecureSkipVerify != nil {
		cfg.Upstream.InsecureSkipVerify = *p.InsecureSkipVerify
	}
	if p.AutoRoute != nil {
		cfg.AutoRoute = *p.AutoRoute
	}
	if cfg.Upstream.Username == "" {
		cfg.Upstream.Username = strings.TrimSpace(p.HTTPBasicUser)
	}
	if cfg.Upstream.Password == "" {
		cfg.Upstream.Password = strings.TrimSpace(p.HTTPBasicPass)
	}
	if p.EnableUDP != nil {
		cfg.Runtime.EnableUDP = *p.EnableUDP
	}
	if p.Routing.Enabled != nil {
		cfg.Routing.Enabled = *p.Routing.Enabled
		cfg.Routing.EnabledSet = true
	}
	for _, entry := range p.Upstreams {
		addr := strings.TrimSpace(entry.Addr)
		if addr == "" {
			continue
		}
		upstream := cfg.Upstream
		upstream.Addr = addr
		if strings.TrimSpace(entry.ServerName) != "" {
			upstream.ServerName = strings.TrimSpace(entry.ServerName)
		}
		if entry.CAPEM != nil {
			upstream.CAPEM = strings.TrimSpace(*entry.CAPEM)
		}
		if entry.Username != nil {
			upstream.Username = strings.TrimSpace(*entry.Username)
		}
		if entry.Password != nil {
			upstream.Password = strings.TrimSpace(*entry.Password)
		}
		cfg.Upstreams = append(cfg.Upstreams, upstream)
	}
	if len(cfg.Upstreams) == 0 && strings.TrimSpace(cfg.Upstream.Addr) != "" {
		cfg.Upstreams = []UpstreamConfig{cfg.Upstream}
	}
	if len(cfg.Upstreams) > 0 {
		cfg.Upstream = cfg.Upstreams[0]
	}
	return cfg, nil
}

/* Fills missing values with safe operational defaults. */
func (c *Config) applyDefaults() {
	if c.Mode == "" {
		c.Mode = ModeSocks5TLS
	}
	if len(c.Upstreams) == 0 && strings.TrimSpace(c.Upstream.Addr) != "" {
		c.Upstreams = []UpstreamConfig{c.Upstream}
	}
	if len(c.Upstreams) > 0 {
		c.Upstream = c.Upstreams[0]
	}
	if c.Tun.Name == "" {
		if runtime.GOOS == "darwin" {
			c.Tun.Name = "utun"
		} else {
			c.Tun.Name = "tun0"
		}
	}
	if c.Tun.MTU <= 0 {
		c.Tun.MTU = 1500
	}
	if c.Runtime.ConnectTimeoutMS <= 0 {
		c.Runtime.ConnectTimeoutMS = 10_000
	}
	if c.Runtime.IdleTimeoutMS <= 0 {
		c.Runtime.IdleTimeoutMS = 300_000
	}
	if c.Runtime.UDPIdleTimeoutMS <= 0 {
		c.Runtime.UDPIdleTimeoutMS = defaultUDPIdleTimeoutMS
	}
	if c.Runtime.TCPBuffer <= 0 {
		c.Runtime.TCPBuffer = 64 * 1024
	}
	if c.Runtime.UDPBuffer <= 0 {
		c.Runtime.UDPBuffer = 64 * 1024
	}
	if c.Runtime.TCPDialConcurrency <= 0 {
		c.Runtime.TCPDialConcurrency = 64
	}
	if c.Runtime.UDPDialConcurrency <= 0 {
		c.Runtime.UDPDialConcurrency = 4
	}
	if c.Runtime.TCPHandlerConcurrency <= 0 {
		c.Runtime.TCPHandlerConcurrency = 512
	}
	if c.Runtime.UDPHandlerConcurrency <= 0 {
		c.Runtime.UDPHandlerConcurrency = 512
	}
	if c.Runtime.DNSCacheSize <= 0 {
		c.Runtime.DNSCacheSize = 1024
	}
	if c.Routing.DefaultOutbound == "" {
		c.Routing.DefaultOutbound = OutboundProxy
	}
	if !c.Routing.EnabledSet {
		c.Routing.Enabled = c.AutoRoute
	}
	if c.Routing.Enabled && len(c.Routing.DirectCIDRs) == 0 && len(c.Routing.DirectCIDRFiles) == 0 {
		c.Routing.DirectCIDRs = append([]string(nil), defaultDirectCIDRs...)
	}
}

/* Applies CLI-level overrides and revalidates the config. */
func (c *Config) ApplyOverrides(mode, tunName string) error {
	if mode != "" {
		c.Mode = mode
	}
	if tunName != "" {
		c.Tun.Name = tunName
	}
	c.applyDefaults()
	return c.Validate()
}

/*
 * Resolves hostname upstreams to a single IP so auto-route bypass and actual
 * dials target the same address. Pinned addresses are static for the process
 * lifetime; restart the process to pick up upstream DNS changes.
 */
func (c *Config) PinUpstreamIPForAutoRoute() (string, error) {
	return c.pinUpstreamIPForAutoRoute(resolveUpstreamIP)
}

func (c *Config) pinUpstreamIPForAutoRoute(resolve func(string) (string, error)) (string, error) {
	if c == nil {
		return "", errors.New("nil config")
	}
	if len(c.Upstreams) == 0 && strings.TrimSpace(c.Upstream.Addr) != "" {
		c.Upstreams = []UpstreamConfig{c.Upstream}
	}
	var pinnedHosts []string
	for i := range c.Upstreams {
		host, err := pinOneUpstreamIP(&c.Upstreams[i], resolve)
		if err != nil {
			return "", err
		}
		if host != "" {
			pinnedHosts = append(pinnedHosts, host)
		}
	}
	if len(c.Upstreams) > 0 {
		c.Upstream = c.Upstreams[0]
	}
	return strings.Join(pinnedHosts, ","), nil
}

func pinOneUpstreamIP(upstream *UpstreamConfig, resolve func(string) (string, error)) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(upstream.Addr))
	if err != nil {
		return "", err
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "", errors.New("empty upstream host")
	}
	if ip := net.ParseIP(host); ip != nil {
		upstream.Addr = net.JoinHostPort(ip.String(), port)
		return "", nil
	}
	if strings.TrimSpace(upstream.ServerName) == "" {
		upstream.ServerName = host
	}
	resolvedIP, err := resolve(host)
	if err != nil {
		return "", err
	}
	upstream.Addr = net.JoinHostPort(resolvedIP, port)
	return host, nil
}

/* Enforces protocol and credential constraints required by runtime. */
func (c *Config) Validate() error {
	if len(c.Upstreams) == 0 && strings.TrimSpace(c.Upstream.Addr) != "" {
		c.Upstreams = []UpstreamConfig{c.Upstream}
	}
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	switch mode {
	case ModeSocks5TLS, ModeHTTPS:
		c.Mode = mode
	default:
		return fmt.Errorf("unsupported mode %q (want %q or %q)", c.Mode, ModeSocks5TLS, ModeHTTPS)
	}
	if len(c.Upstreams) == 0 {
		return errors.New("upstreams is required")
	}
	for i := range c.Upstreams {
		if err := validateUpstreamAddr(c.Upstreams[i].Addr); err != nil {
			return fmt.Errorf("upstreams[%d]: %w", i, err)
		}
		if err := validateUpstreamCredentials(c.Mode, c.Upstreams[i]); err != nil {
			return fmt.Errorf("upstreams[%d]: %w", i, err)
		}
	}
	c.Upstream = c.Upstreams[0]
	tunName := strings.TrimSpace(c.Tun.Name)
	if tunName == "" {
		return errors.New("tun.name is required")
	}
	switch runtime.GOOS {
	case "linux":
		if len(tunName) > 15 {
			return fmt.Errorf("tun.name too long for linux (max 15): %q", tunName)
		}
	case "darwin":
		if _, err := parseUTUNUnit(tunName); err != nil {
			return fmt.Errorf("invalid tun.name for darwin: %w", err)
		}
	default:
		return fmt.Errorf("unsupported runtime os %q (only linux/darwin)", runtime.GOOS)
	}
	if c.Tun.MTU < 576 {
		return fmt.Errorf("tun.mtu too small: %d", c.Tun.MTU)
	}
	if strings.TrimSpace(c.Tun.IPv6) != "" {
		if !strings.Contains(c.Tun.IPv6, "/") {
			ip := net.ParseIP(strings.TrimSpace(c.Tun.IPv6))
			if ip == nil || ip.To16() == nil || ip.To4() != nil {
				return fmt.Errorf("tun.ipv6 must be an IPv6 address or CIDR, got %q", c.Tun.IPv6)
			}
		} else {
			ip, _, err := net.ParseCIDR(strings.TrimSpace(c.Tun.IPv6))
			if err != nil || ip == nil || ip.To16() == nil || ip.To4() != nil {
				return fmt.Errorf("tun.ipv6 must be an IPv6 address or CIDR, got %q", c.Tun.IPv6)
			}
		}
	}
	if c.Runtime.ConnectTimeoutMS < minConnectTimeoutMS || c.Runtime.ConnectTimeoutMS > maxConnectTimeoutMS {
		return fmt.Errorf(
			"runtime.connect_timeout_ms out of range [%d,%d]: %d",
			minConnectTimeoutMS,
			maxConnectTimeoutMS,
			c.Runtime.ConnectTimeoutMS,
		)
	}
	if c.Runtime.IdleTimeoutMS < minIdleTimeoutMS || c.Runtime.IdleTimeoutMS > maxIdleTimeoutMS {
		return fmt.Errorf(
			"runtime.idle_timeout_ms out of range [%d,%d]: %d",
			minIdleTimeoutMS,
			maxIdleTimeoutMS,
			c.Runtime.IdleTimeoutMS,
		)
	}
	if c.Runtime.UDPIdleTimeoutMS < minIdleTimeoutMS || c.Runtime.UDPIdleTimeoutMS > maxIdleTimeoutMS {
		return fmt.Errorf(
			"runtime.udp_idle_timeout_ms out of range [%d,%d]: %d",
			minIdleTimeoutMS,
			maxIdleTimeoutMS,
			c.Runtime.UDPIdleTimeoutMS,
		)
	}
	if c.Runtime.TCPBuffer < minRuntimeBufferBytes || c.Runtime.TCPBuffer > maxRuntimeBufferBytes {
		return fmt.Errorf(
			"runtime.tcp_buffer out of range [%d,%d]: %d",
			minRuntimeBufferBytes,
			maxRuntimeBufferBytes,
			c.Runtime.TCPBuffer,
		)
	}
	if c.Runtime.UDPBuffer < minRuntimeBufferBytes || c.Runtime.UDPBuffer > maxRuntimeBufferBytes {
		return fmt.Errorf(
			"runtime.udp_buffer out of range [%d,%d]: %d",
			minRuntimeBufferBytes,
			maxRuntimeBufferBytes,
			c.Runtime.UDPBuffer,
		)
	}
	if c.Runtime.TCPDialConcurrency <= 0 || c.Runtime.TCPDialConcurrency > maxDialConcurrency {
		return fmt.Errorf(
			"runtime.tcp_dial_concurrency out of range [1,%d]: %d",
			maxDialConcurrency,
			c.Runtime.TCPDialConcurrency,
		)
	}
	if c.Runtime.UDPDialConcurrency <= 0 || c.Runtime.UDPDialConcurrency > maxDialConcurrency {
		return fmt.Errorf(
			"runtime.udp_dial_concurrency out of range [1,%d]: %d",
			maxDialConcurrency,
			c.Runtime.UDPDialConcurrency,
		)
	}
	if c.Runtime.TCPHandlerConcurrency <= 0 || c.Runtime.TCPHandlerConcurrency > maxHandlerConcurrency {
		return fmt.Errorf(
			"runtime.tcp_handler_concurrency out of range [1,%d]: %d",
			maxHandlerConcurrency,
			c.Runtime.TCPHandlerConcurrency,
		)
	}
	if c.Runtime.UDPHandlerConcurrency <= 0 || c.Runtime.UDPHandlerConcurrency > maxHandlerConcurrency {
		return fmt.Errorf(
			"runtime.udp_handler_concurrency out of range [1,%d]: %d",
			maxHandlerConcurrency,
			c.Runtime.UDPHandlerConcurrency,
		)
	}
	if c.Runtime.DNSCacheSize <= 0 || c.Runtime.DNSCacheSize > maxDNSCacheEntries {
		return fmt.Errorf(
			"runtime.dns_cache_size out of range [1,%d]: %d",
			maxDNSCacheEntries,
			c.Runtime.DNSCacheSize,
		)
	}
	if estimated := c.EstimatedRelayBufferBytes(); estimated > maxActiveRelayBufferEstimate {
		return fmt.Errorf(
			"active relay buffer estimate exceeds %d bytes: estimated %d bytes from configured buffers and handler concurrency",
			maxActiveRelayBufferEstimate,
			estimated,
		)
	}
	if err := c.Routing.Validate(); err != nil {
		return err
	}
	return nil
}

type jsonValueSchema struct {
	kind         jsonValueKind
	objectFields map[string]*jsonValueSchema
	arrayItem    *jsonValueSchema
}

type jsonValueKind uint8

const (
	jsonKindAny jsonValueKind = iota
	jsonKindObject
	jsonKindArray
	jsonKindString
	jsonKindNumber
	jsonKindBool
)

var (
	jsonStringSchema   = &jsonValueSchema{kind: jsonKindString}
	jsonNumberSchema   = &jsonValueSchema{kind: jsonKindNumber}
	jsonBoolSchema     = &jsonValueSchema{kind: jsonKindBool}
	upstreamJSONSchema = &jsonValueSchema{kind: jsonKindObject, objectFields: map[string]*jsonValueSchema{
		"addr":        jsonStringSchema,
		"server_name": jsonStringSchema,
		"ca_pem":      jsonStringSchema,
		"username":    jsonStringSchema,
		"password":    jsonStringSchema,
	}}
	routingJSONSchema = &jsonValueSchema{kind: jsonKindObject, objectFields: map[string]*jsonValueSchema{
		"enabled":           jsonBoolSchema,
		"default":           jsonStringSchema,
		"direct_cidrs":      {kind: jsonKindArray, arrayItem: jsonStringSchema},
		"proxy_cidrs":       {kind: jsonKindArray, arrayItem: jsonStringSchema},
		"direct_cidr_files": {kind: jsonKindArray, arrayItem: jsonStringSchema},
		"proxy_cidr_files":  {kind: jsonKindArray, arrayItem: jsonStringSchema},
	}}
	configJSONSchema = &jsonValueSchema{kind: jsonKindObject, objectFields: map[string]*jsonValueSchema{
		"mode":                 jsonStringSchema,
		"auto_route":           jsonBoolSchema,
		"tun":                  jsonStringSchema,
		"tun_mtu":              jsonNumberSchema,
		"tun_ipv4":             jsonStringSchema,
		"tun_ipv6":             jsonStringSchema,
		"upstream":             jsonStringSchema,
		"upstreams":            {kind: jsonKindArray, arrayItem: upstreamJSONSchema},
		"client_pem":           jsonStringSchema,
		"client_key":           jsonStringSchema,
		"ca_pem":               jsonStringSchema,
		"insecure_skip_verify": jsonBoolSchema,
		"username":             jsonStringSchema,
		"password":             jsonStringSchema,
		"http_basic_user":      jsonStringSchema,
		"http_basic_pass":      jsonStringSchema,
		"connect_timeout_ms":   jsonNumberSchema,
		"idle_timeout_ms":      jsonNumberSchema,
		"udp_idle_timeout_ms":  jsonNumberSchema,
		"enable_udp":           jsonBoolSchema,
		"tcp_dial_concurrency": jsonNumberSchema,
		"udp_dial_concurrency": jsonNumberSchema,
		"dns_cache_size":       jsonNumberSchema,
		"routing":              routingJSONSchema,
	}}
)

/* Rejects inexact or duplicate object keys at every configured nesting level. */
func validateConfigJSONShape(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSONValue(decoder, "$", configJSONSchema); err != nil {
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, path string, schema *jsonValueSchema) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	actualKind, kindName := classifyJSONToken(token)
	if schema != nil && schema.kind != jsonKindAny && actualKind != schema.kind {
		return fmt.Errorf("invalid JSON type at %s: got %s, want %s", path, kindName, schema.kind)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("invalid object key at %s", path)
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON field %q at %s", key, path)
			}
			seen[key] = struct{}{}
			var childSchema *jsonValueSchema
			if schema != nil && schema.objectFields != nil {
				var exists bool
				childSchema, exists = schema.objectFields[key]
				if !exists {
					return fmt.Errorf("unknown field %q at %s", key, path)
				}
			}
			if err := scanJSONValue(decoder, path+"."+key, childSchema); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for index := 0; decoder.More(); index++ {
			var itemSchema *jsonValueSchema
			if schema != nil {
				itemSchema = schema.arrayItem
			}
			if err := scanJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), itemSchema); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected JSON delimiter %q at %s", delim, path)
	}
}

func classifyJSONToken(token any) (jsonValueKind, string) {
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return jsonKindObject, "object"
		case '[':
			return jsonKindArray, "array"
		}
	case string:
		return jsonKindString, "string"
	case json.Number, float64:
		return jsonKindNumber, "number"
	case bool:
		return jsonKindBool, "boolean"
	case nil:
		return jsonKindAny, "null"
	}
	return jsonKindAny, fmt.Sprintf("%T", token)
}

func (k jsonValueKind) String() string {
	switch k {
	case jsonKindObject:
		return "object"
	case jsonKindArray:
		return "array"
	case jsonKindString:
		return "string"
	case jsonKindNumber:
		return "number"
	case jsonKindBool:
		return "boolean"
	default:
		return "value"
	}
}

/* Returns the worst-case backing-array capacity held by active relay flows. */
func (c *Config) EstimatedRelayBufferBytes() int64 {
	if c == nil {
		return 0
	}
	total := int64(2) * relayBufferAllocationBytes(c.Runtime.TCPBuffer) * int64(c.Runtime.TCPHandlerConcurrency)
	if c.Runtime.EnableUDP {
		perUDPFlow := int64(2) * relayBufferAllocationBytes(c.Runtime.UDPBuffer)
		if c.Mode == ModeHTTPS {
			/* DNS-over-TCP can simultaneously hold its ingress, framed query, and response arrays. */
			perUDPFlow = relayBufferAllocationBytes(c.Runtime.UDPBuffer) +
				relayBufferAllocationBytes(c.Runtime.UDPBuffer+2) +
				relayBufferAllocationBytes(maxDNSOverTCPExtraBytes-2)
		} else {
			/* A proxied SOCKS5 UDP flow also owns read/write framing buffers. */
			sessionBuffer := c.Runtime.UDPBuffer + 512
			if sessionBuffer < 64*1024+512 {
				sessionBuffer = 64*1024 + 512
			}
			perUDPFlow += int64(2 * sessionBuffer)
		}
		total += perUDPFlow * int64(c.Runtime.UDPHandlerConcurrency)
	}
	return total
}

func relayBufferAllocationBytes(size int) int64 {
	_, classSize, pooled := relayBufferClass(size)
	if pooled {
		return int64(classSize)
	}
	return int64(size)
}

func (r *RoutingConfig) Validate() error {
	r.DefaultOutbound = strings.ToLower(strings.TrimSpace(r.DefaultOutbound))
	if r.DefaultOutbound == "" {
		r.DefaultOutbound = OutboundProxy
	}
	switch r.DefaultOutbound {
	case OutboundDirect, OutboundProxy:
	default:
		return fmt.Errorf("routing.default must be %q or %q, got %q", OutboundDirect, OutboundProxy, r.DefaultOutbound)
	}
	for i, cidr := range r.DirectCIDRs {
		if _, _, err := net.ParseCIDR(strings.TrimSpace(cidr)); err != nil {
			return fmt.Errorf("routing.direct_cidrs[%d]: invalid CIDR %q: %w", i, cidr, err)
		}
	}
	for i, cidr := range r.ProxyCIDRs {
		if _, _, err := net.ParseCIDR(strings.TrimSpace(cidr)); err != nil {
			return fmt.Errorf("routing.proxy_cidrs[%d]: invalid CIDR %q: %w", i, cidr, err)
		}
	}
	return nil
}

func validateUpstreamAddr(addr string) error {
	if strings.TrimSpace(addr) == "" {
		return errors.New("addr is required")
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("addr must be host:port, got %q: %w", addr, err)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("addr host is empty: %q", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("addr has invalid port %q", portStr)
	}
	return nil
}

func validateUpstreamCredentials(mode string, upstream UpstreamConfig) error {
	if caPEMPath := strings.TrimSpace(upstream.CAPEM); caPEMPath != "" {
		if _, err := os.Stat(caPEMPath); err != nil {
			return fmt.Errorf("ca_pem is not accessible: %w", err)
		}
	}
	httpsWithBasicAuth := mode == ModeHTTPS && upstream.hasBasicAuthCredentials()
	clientPEMPath, clientKeyPath := upstream.trimmedClientCertificatePaths()
	if !httpsWithBasicAuth && (clientPEMPath == "" || clientKeyPath == "") {
		return errors.New("client_pem and client_key are required")
	}
	if clientPEMPath == "" && clientKeyPath == "" {
		return nil
	}
	if clientPEMPath == "" || clientKeyPath == "" {
		return errors.New("client_pem and client_key must both be set")
	}
	if _, err := os.Stat(clientPEMPath); err != nil {
		return fmt.Errorf("client_pem is not accessible: %w", err)
	}
	if _, err := os.Stat(clientKeyPath); err != nil {
		return fmt.Errorf("client_key is not accessible: %w", err)
	}
	return nil
}

func trimStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

/* Returns the effective outbound connect timeout. */
func (c *Config) ConnectTimeout() time.Duration {
	return time.Duration(c.Runtime.ConnectTimeoutMS) * time.Millisecond
}

/* Returns the effective idle timeout for flow relays. */
func (c *Config) IdleTimeout() time.Duration {
	return time.Duration(c.Runtime.IdleTimeoutMS) * time.Millisecond
}

/* Returns the timeout budget used by DNS-over-TCP exchanges. */
func (c *Config) DNSExchangeTimeout() time.Duration {
	connectTimeout := c.ConnectTimeout()
	idleTimeout := c.IdleTimeout()
	if connectTimeout <= 0 {
		return idleTimeout
	}
	if idleTimeout > 0 && idleTimeout < connectTimeout {
		return idleTimeout
	}
	return connectTimeout
}

/* Returns the idle timeout used for generic UDP relay flows. */
func (c *Config) UDPIdleTimeout() time.Duration {
	idle := time.Duration(c.Runtime.UDPIdleTimeoutMS) * time.Millisecond
	if idle <= 0 {
		idle = defaultUDPIdleTimeoutMS * time.Millisecond
	}
	return idle
}
