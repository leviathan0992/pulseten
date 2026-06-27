package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	OutboundDirect = "direct"
	OutboundProxy  = "proxy"
)

type RouteDecision struct {
	Outbound string
	Rule     string
}

type DirectRoute struct {
	IP       net.IP
	Decision RouteDecision
}

type Router struct {
	enabled         bool
	defaultOutbound string
	defaultRule     string
	directCIDRMatch *cidrMatcher
	proxyCIDRMatch  *cidrMatcher
}

func NewRouter(cfg RoutingConfig) (*Router, error) {
	cfg.DirectCIDRs = trimStringSlice(cfg.DirectCIDRs)
	cfg.ProxyCIDRs = trimStringSlice(cfg.ProxyCIDRs)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Enabled {
		return &Router{
			enabled:         false,
			defaultOutbound: cfg.DefaultOutbound,
			defaultRule:     "default:" + cfg.DefaultOutbound,
			directCIDRMatch: newLabeledCIDRMatcher(nil, "direct_cidr:"),
			proxyCIDRMatch:  newLabeledCIDRMatcher(nil, "proxy_cidr:"),
		}, nil
	}

	directCIDRs, err := loadCIDRRules(cfg.DirectCIDRs, cfg.DirectCIDRFiles)
	if err != nil {
		return nil, fmt.Errorf("load direct CIDR rules: %w", err)
	}
	proxyCIDRs, err := loadCIDRRules(cfg.ProxyCIDRs, cfg.ProxyCIDRFiles)
	if err != nil {
		return nil, fmt.Errorf("load proxy CIDR rules: %w", err)
	}
	return &Router{
		enabled:         cfg.Enabled,
		defaultOutbound: cfg.DefaultOutbound,
		defaultRule:     "default:" + cfg.DefaultOutbound,
		directCIDRMatch: newLabeledCIDRMatcher(directCIDRs, "direct_cidr:"),
		proxyCIDRMatch:  newLabeledCIDRMatcher(proxyCIDRs, "proxy_cidr:"),
	}, nil
}

func (r *Router) Enabled() bool {
	return r != nil && r.enabled
}

func (r *Router) DecideIP(ip net.IP) RouteDecision {
	if r == nil || !r.enabled || ip == nil {
		return RouteDecision{Outbound: OutboundProxy, Rule: "routing-disabled"}
	}
	if ip := normalizeIP(ip); ip != nil {
		if rule, ok := r.proxyCIDRMatch.Match(ip); ok {
			return RouteDecision{Outbound: OutboundProxy, Rule: rule}
		}
		if rule, ok := r.directCIDRMatch.Match(ip); ok {
			return RouteDecision{Outbound: OutboundDirect, Rule: rule}
		}
	}
	return RouteDecision{Outbound: r.defaultOutbound, Rule: r.defaultRule}
}

type cidrMatcher struct {
	v4         *cidrTrieNode
	v6         *cidrTrieNode
	rulePrefix string
}

type cidrTrieNode struct {
	rule  string
	child [2]*cidrTrieNode
}

func newCIDRMatcher(cidrs []*net.IPNet) *cidrMatcher {
	return newLabeledCIDRMatcher(cidrs, "")
}

func newLabeledCIDRMatcher(cidrs []*net.IPNet, rulePrefix string) *cidrMatcher {
	m := &cidrMatcher{rulePrefix: rulePrefix}
	for _, cidr := range cidrs {
		m.Insert(cidr)
	}
	return m
}

func (m *cidrMatcher) Insert(cidr *net.IPNet) {
	if m == nil || cidr == nil {
		return
	}
	ip, bits := cidrBaseIP(cidr)
	if ip == nil {
		return
	}
	ones, maskBits := cidr.Mask.Size()
	if ones < 0 || maskBits != bits {
		return
	}
	root := &m.v4
	if bits == 128 {
		root = &m.v6
	}
	if *root == nil {
		*root = &cidrTrieNode{}
	}
	node := *root
	for bit := 0; bit < ones; bit++ {
		idx := ipBit(ip, bit)
		if node.child[idx] == nil {
			node.child[idx] = &cidrTrieNode{}
		}
		node = node.child[idx]
	}
	node.rule = m.rulePrefix + cidr.String()
}

func (m *cidrMatcher) Match(ip net.IP) (string, bool) {
	if m == nil {
		return "", false
	}
	ip = normalizeIP(ip)
	if ip == nil {
		return "", false
	}
	bits := 128
	root := m.v6
	if v4 := ip.To4(); v4 != nil {
		ip = v4
		bits = 32
		root = m.v4
	}
	if root == nil {
		return "", false
	}
	node := root
	rule := node.rule
	for bit := 0; bit < bits; bit++ {
		next := node.child[ipBit(ip, bit)]
		if next == nil {
			break
		}
		node = next
		if node.rule != "" {
			rule = node.rule
		}
	}
	return rule, rule != ""
}

func cidrBaseIP(cidr *net.IPNet) (net.IP, int) {
	if cidr == nil {
		return nil, 0
	}
	if ip := cidr.IP.To4(); ip != nil {
		return ip, 32
	}
	if ip := cidr.IP.To16(); ip != nil {
		return ip, 128
	}
	return nil, 0
}

func ipBit(ip net.IP, bit int) int {
	if ip[bit/8]&(1<<(7-uint(bit%8))) == 0 {
		return 0
	}
	return 1
}

func loadCIDRRules(inline, files []string) ([]*net.IPNet, error) {
	values := append([]string{}, inline...)
	for _, path := range files {
		lines, err := readRuleLines(path)
		if err != nil {
			return nil, err
		}
		values = append(values, lines...)
	}
	var out []*net.IPNet
	seen := make(map[string]struct{})
	for _, value := range values {
		_, cidr, err := net.ParseCIDR(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("%q: %w", value, err)
		}
		key := cidr.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, cidr)
	}
	return out, nil
}

func readRuleLines(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var out []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeIP(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return v4
	}
	return ip.To16()
}
