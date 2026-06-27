package main

import (
	"net"
	"testing"
)

func BenchmarkRouterDecideIPDefaultRules(b *testing.B) {
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     defaultDirectCIDRs,
	})
	if err != nil {
		b.Fatalf("build router: %v", err)
	}
	ip := net.ParseIP("1.0.1.8")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decision := router.DecideIP(ip)
		if decision.Outbound != OutboundDirect {
			b.Fatalf("unexpected decision: %+v", decision)
		}
	}
}

func BenchmarkRouterDecideIPDefaultOutbound(b *testing.B) {
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     defaultDirectCIDRs,
	})
	if err != nil {
		b.Fatalf("build router: %v", err)
	}
	for _, ip := range []net.IP{net.ParseIP("203.0.113.8"), net.ParseIP("2001:db8::8")} {
		b.Run(ip.String(), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				decision := router.DecideIP(ip)
				if decision.Rule != "default:proxy" {
					b.Fatalf("unexpected decision: %+v", decision)
				}
			}
		})
	}
}

func BenchmarkEncodeSocks5UDPAddressInto(b *testing.B) {
	target := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}
	buf := make([]byte, 19)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := encodeSocks5UDPAddressInto(buf, target); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRelayBufferPool64KiB(b *testing.B) {
	const size = 64 * 1024
	var pool relayBufferPool
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := getPooledBuf(&pool, size)
		putPooledBuf(&pool, buf)
	}
}
