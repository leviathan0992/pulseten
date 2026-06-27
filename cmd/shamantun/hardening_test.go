package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDecodeConfigRejectsUnknownFields(t *testing.T) {
	_, err := decodeConfig([]byte(`{"mode":"https","upstreams":[{"addr":"127.0.0.1:443"}],"enable_upd":true}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown field error, got: %v", err)
	}
}

func TestDecodeConfigRejectsNestedUnknownFields(t *testing.T) {
	_, err := decodeConfig([]byte(`{"mode":"https","upstreams":[{"addr":"127.0.0.1:443","usernmae":"x"}]}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected nested unknown field error, got: %v", err)
	}
}

func TestDecodeConfigRejectsDuplicateFields(t *testing.T) {
	for _, raw := range []string{
		`{"mode":"https","mode":"socks5tls"}`,
		`{"upstreams":[{"addr":"127.0.0.1:443","addr":"127.0.0.2:443"}]}`,
	} {
		_, err := decodeConfig([]byte(raw))
		if err == nil || !strings.Contains(err.Error(), "duplicate JSON field") {
			t.Fatalf("expected duplicate field error for %s, got: %v", raw, err)
		}
	}
}

func TestDecodeConfigRejectsInexactAndCaseCollidingFields(t *testing.T) {
	for _, raw := range []string{
		`{"Mode":"https"}`,
		`{"mode":"https","Mode":"socks5tls"}`,
		`{"routing":{"Enabled":true}}`,
		`{"upstreams":[{"Addr":"127.0.0.1:443"}]}`,
		`{"upstreams":[{"addr":"127.0.0.1:443","Addr":"127.0.0.2:443"}]}`,
	} {
		_, err := decodeConfig([]byte(raw))
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("expected inexact field error for %s, got: %v", raw, err)
		}
	}
}

func TestDecodeConfigRejectsTrailingJSON(t *testing.T) {
	_, err := decodeConfig([]byte(`{"mode":"https"} {"mode":"socks5tls"}`))
	if err == nil {
		t.Fatal("expected trailing JSON document to be rejected")
	}
}

func TestDecodeConfigRejectsNullAndWrongJSONTypes(t *testing.T) {
	for _, raw := range []string{
		`{"mode":null}`,
		`{"mode":1}`,
		`{"auto_route":null}`,
		`{"connect_timeout_ms":"10000"}`,
		`{"upstreams":null}`,
		`{"upstreams":[null]}`,
		`{"upstreams":[{"addr":false}]}`,
		`{"routing":null}`,
		`{"routing":{"enabled":null}}`,
		`{"routing":{"direct_cidrs":[1]}}`,
	} {
		_, err := decodeConfig([]byte(raw))
		if err == nil || !strings.Contains(err.Error(), "invalid JSON type") {
			t.Fatalf("expected strict type error for %s, got: %v", raw, err)
		}
	}
}

func TestConfigValidateRejectsExcessiveActiveRelayBufferEstimate(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.EnableUDP = true
	cfg.Runtime.TCPBuffer = maxRuntimeBufferBytes
	cfg.Runtime.UDPBuffer = maxRuntimeBufferBytes
	cfg.Runtime.TCPHandlerConcurrency = maxHandlerConcurrency
	cfg.Runtime.UDPHandlerConcurrency = maxHandlerConcurrency

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "active relay buffer estimate") {
		t.Fatalf("expected active relay buffer estimate error, got: %v", err)
	}
}

func TestDecodeConfigRejectsHiddenRuntimeTuningFields(t *testing.T) {
	for _, raw := range []string{
		`{"config_version":2}`,
		`{"tcp_buffer":65536}`,
		`{"udp_buffer":65536}`,
		`{"tcp_handler_concurrency":512}`,
		`{"udp_handler_concurrency":512}`,
		`{"allow_proxied_udp_443":false}`,
	} {
		_, err := decodeConfig([]byte(raw))
		if err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("expected hidden runtime field to be rejected for %s, got: %v", raw, err)
		}
	}
}

func TestEstimatedRelayBufferBytesIncludesSocksSessionBuffers(t *testing.T) {
	cfg := &Config{Runtime: RuntimeConfig{
		TCPBuffer:             4096,
		UDPBuffer:             4096,
		TCPHandlerConcurrency: 1,
		UDPHandlerConcurrency: 1,
		EnableUDP:             true,
	}}
	const want = int64(148480)
	if got := cfg.EstimatedRelayBufferBytes(); got != want {
		t.Fatalf("unexpected relay memory estimate: got=%d want=%d", got, want)
	}
}

func TestEstimatedRelayBufferBytesUsesHTTPSDNSBuffers(t *testing.T) {
	cfg := &Config{
		Mode: ModeHTTPS,
		Runtime: RuntimeConfig{
			UDPBuffer:             4096,
			UDPHandlerConcurrency: 1,
			EnableUDP:             true,
		},
	}
	const want = int64(77824)
	if got := cfg.EstimatedRelayBufferBytes(); got != want {
		t.Fatalf("unexpected HTTPS relay memory estimate: got=%d want=%d", got, want)
	}
}

func TestEstimatedRelayBufferBytesUsesPooledClassCapacity(t *testing.T) {
	cfg := &Config{Runtime: RuntimeConfig{
		TCPBuffer:             128*1024 + 1,
		TCPHandlerConcurrency: maxHandlerConcurrency - 1,
	}}
	const want = int64(2 * 256 * 1024 * (maxHandlerConcurrency - 1))
	if got := cfg.EstimatedRelayBufferBytes(); got != want {
		t.Fatalf("unexpected pooled-capacity estimate: got=%d want=%d", got, want)
	}
	if got := cfg.EstimatedRelayBufferBytes(); got <= maxActiveRelayBufferEstimate {
		t.Fatalf("expected actual pooled capacity to exceed guard: got=%d limit=%d", got, maxActiveRelayBufferEstimate)
	}
}

func TestRelayBufferAllocationBytesRoundsEveryPoolBoundary(t *testing.T) {
	for _, tc := range []struct {
		requested int
		want      int64
	}{
		{4*1024 + 1, 8 * 1024},
		{8*1024 + 1, 16 * 1024},
		{16*1024 + 1, 32 * 1024},
		{32*1024 + 1, 64 * 1024},
		{64*1024 + 1, 128 * 1024},
		{128*1024 + 1, 256 * 1024},
		{256*1024 + 1, 256*1024 + 1},
	} {
		if got := relayBufferAllocationBytes(tc.requested); got != tc.want {
			t.Fatalf("unexpected allocation size for %d: got=%d want=%d", tc.requested, got, tc.want)
		}
	}
}

func TestPutPooledBufDropsOversizedBuffers(t *testing.T) {
	var pool relayBufferPool
	buf := make([]byte, maxPooledRelayBufferBytes+1)
	putPooledBuf(&pool, &buf)
	for i := range pool.classes {
		if got := pool.classes[i].Get(); got != nil {
			t.Fatalf("expected oversized buffer to be dropped, class=%d got=%T", i, got)
		}
	}
}

func TestRelayBufferPoolReuseDoesNotAllocate(t *testing.T) {
	var pool relayBufferPool
	buf := getPooledBuf(&pool, 64*1024)
	putPooledBuf(&pool, buf)
	allocs := testing.AllocsPerRun(1000, func() {
		buf := getPooledBuf(&pool, 64*1024)
		putPooledBuf(&pool, buf)
	})
	if allocs != 0 {
		t.Fatalf("expected allocation-free relay buffer reuse, got %.2f allocs/op", allocs)
	}
}

func TestRouterDecisionDoesNotAllocate(t *testing.T) {
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"1.0.1.0/24"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	for name, ip := range map[string]net.IP{
		"matched-ipv4": net.ParseIP("1.0.1.8"),
		"default-ipv4": net.ParseIP("203.0.113.8"),
		"default-ipv6": net.ParseIP("2001:db8::8"),
	} {
		t.Run(name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				decision := router.DecideIP(ip)
				if decision.Rule == "" {
					t.Fatalf("unexpected decision: %+v", decision)
				}
			})
			if allocs != 0 {
				t.Fatalf("expected allocation-free route decision, got %.2f allocs/op", allocs)
			}
		})
	}
}

func TestDNSCacheEnforcesPayloadByteBudget(t *testing.T) {
	e := &Engine{
		cfg:              &Config{Runtime: RuntimeConfig{DNSCacheSize: 8}},
		dnsCache:         make(map[string]dnsCacheEntry),
		dnsCacheMaxBytes: 120,
	}
	req := []byte{
		0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
	}
	resp := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04, 0x01, 0x01, 0x01, 0x01,
	}

	e.putDNSCache("1.1.1.1:53", req, resp)
	e.putDNSCache("8.8.8.8:53", req, resp)
	if e.dnsCacheBytes > e.dnsCacheMaxBytes {
		t.Fatalf("dns cache exceeded byte budget: got=%d max=%d", e.dnsCacheBytes, e.dnsCacheMaxBytes)
	}
	if len(e.dnsCache) != 1 {
		t.Fatalf("expected byte budget to retain one entry, got %d", len(e.dnsCache))
	}
}

func TestDNSCacheAccountingMutationPaths(t *testing.T) {
	req, resp := cacheableDNSMessages()
	entrySize := func(target string) int64 {
		return int64(len(dnsCacheKey(target, req)) + len(resp))
	}
	assertAccounting := func(t *testing.T, e *Engine, wantEntries int, wantBytes int64) {
		t.Helper()
		if len(e.dnsCache) != wantEntries || e.dnsCacheBytes != wantBytes {
			t.Fatalf("unexpected cache accounting: entries=%d/%d bytes=%d/%d", len(e.dnsCache), wantEntries, e.dnsCacheBytes, wantBytes)
		}
		var summed int64
		for _, entry := range e.dnsCache {
			summed += entry.SizeBytes
		}
		if summed != e.dnsCacheBytes {
			t.Fatalf("counter differs from entries: counter=%d summed=%d", e.dnsCacheBytes, summed)
		}
	}

	t.Run("replacement-and-expiry", func(t *testing.T) {
		target := "1.1.1.1:53"
		e := &Engine{cfg: &Config{Runtime: RuntimeConfig{DNSCacheSize: 2}}, dnsCache: make(map[string]dnsCacheEntry)}
		e.putDNSCache(target, req, resp)
		want := entrySize(target)
		assertAccounting(t, e, 1, want)
		e.putDNSCache(target, req, resp)
		assertAccounting(t, e, 1, want)
		key := dnsCacheKey(target, req)
		entry := e.dnsCache[key]
		entry.ExpiresAt = time.Now().Add(-time.Second)
		e.dnsCache[key] = entry
		if got := e.getDNSCache(target, req); got != nil {
			t.Fatalf("expected expired cache miss, got %x", got)
		}
		assertAccounting(t, e, 0, 0)
	})

	t.Run("count-eviction", func(t *testing.T) {
		e := &Engine{cfg: &Config{Runtime: RuntimeConfig{DNSCacheSize: 1}}, dnsCache: make(map[string]dnsCacheEntry)}
		e.putDNSCache("1.1.1.1:53", req, resp)
		e.putDNSCache("8.8.8.8:53", req, resp)
		assertAccounting(t, e, 1, entrySize("8.8.8.8:53"))
	})

	t.Run("byte-eviction", func(t *testing.T) {
		firstSize := entrySize("1.1.1.1:53")
		e := &Engine{
			cfg:              &Config{Runtime: RuntimeConfig{DNSCacheSize: 2}},
			dnsCache:         make(map[string]dnsCacheEntry),
			dnsCacheMaxBytes: firstSize + entrySize("8.8.8.8:53") - 1,
		}
		e.putDNSCache("1.1.1.1:53", req, resp)
		e.putDNSCache("8.8.8.8:53", req, resp)
		assertAccounting(t, e, 1, entrySize("8.8.8.8:53"))
	})

	t.Run("oversized-rejection", func(t *testing.T) {
		e := &Engine{
			cfg:              &Config{Runtime: RuntimeConfig{DNSCacheSize: 2}},
			dnsCache:         make(map[string]dnsCacheEntry),
			dnsCacheMaxBytes: entrySize("1.1.1.1:53") - 1,
		}
		e.putDNSCache("1.1.1.1:53", req, resp)
		assertAccounting(t, e, 0, 0)
	})
}

func TestDNSPoolEnforcesGlobalIdleLimitAndExpiresEntries(t *testing.T) {
	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool:         make(map[string][]idleDNSConn),
	}
	for i := 0; i < dnsPoolMaxIdle; i++ {
		conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: i + 1}}
		e.registerActiveUpstream(conn)
		e.releaseDNSConn(net.JoinHostPort("192.0.2.1", fmt.Sprint(i+1)), conn, 0)
	}
	e.dnsPoolMu.Lock()
	pooled := e.dnsPoolSizeLocked()
	e.dnsPoolMu.Unlock()
	if pooled != dnsPoolMaxIdle {
		t.Fatalf("unexpected global dns idle count: got=%d want=%d", pooled, dnsPoolMaxIdle)
	}

	overflow := &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6553}}
	e.registerActiveUpstream(overflow)
	e.releaseDNSConn("198.51.100.1:53", overflow, 0)
	if !overflow.isClosed() {
		t.Fatal("expected dns connection beyond global idle limit to be closed")
	}

	expired := &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53}}
	e.dnsPoolMu.Lock()
	e.dnsPool = map[string][]idleDNSConn{
		"203.0.113.1:53": {{conn: expired, expiresAt: time.Now().Add(-time.Second)}},
	}
	e.dnsPoolMu.Unlock()
	fresh := &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54}}
	e.registerActiveUpstream(fresh)
	e.releaseDNSConn("203.0.113.2:53", fresh, 0)
	if !expired.isClosed() {
		t.Fatal("expected expired idle dns connection to be closed during bounded pool maintenance")
	}
}

func TestNetworkGenerationRejectsLateActivation(t *testing.T) {
	e := &Engine{activeUpstreams: make(map[io.Closer]struct{})}
	generation := e.networkGeneration.Load()
	conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}}
	e.upstreamLifecycleMu.Lock()
	e.resetDialContext()
	e.closeActiveUpstreams()
	e.upstreamLifecycleMu.Unlock()
	if e.activateUpstreamForGeneration(conn, generation) {
		t.Fatal("expected old-generation dial result to be rejected")
	}
	if !conn.isClosed() {
		t.Fatal("expected old-generation dial result to be closed")
	}
}

func TestNetworkResetRejectsLateDNSAndUDPReleases(t *testing.T) {
	t.Run("dns", func(t *testing.T) {
		reached := make(chan struct{}, 1)
		conn := &closeTrackingConn{
			remote:     &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443},
			deadlineCh: reached,
		}
		e := &Engine{
			activeUpstreams: map[io.Closer]struct{}{conn: {}},
			dnsPool:         make(map[string][]idleDNSConn),
		}
		e.upstreamLifecycleMu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.releaseDNSConn("8.8.8.8:53", conn, 0)
		}()
		<-reached
		e.resetDialContext()
		e.closeIdleDNSPool()
		e.closeActiveUpstreams()
		e.upstreamLifecycleMu.Unlock()
		<-done
		if !conn.isClosed() || len(e.dnsPool) != 0 {
			t.Fatalf("late dns release survived reset: closed=%v pool=%d", conn.isClosed(), len(e.dnsPool))
		}
	})

	t.Run("udp", func(t *testing.T) {
		reached := make(chan struct{}, 1)
		session := &deadlineAwareUDPSession{
			target:     &net.UDPAddr{IP: net.IPv4(9, 9, 9, 9), Port: 9999},
			deadlineCh: reached,
		}
		e := &Engine{
			activeUpstreams: map[io.Closer]struct{}{session: {}},
			udpPool:         make(map[string][]UDPSession),
		}
		e.upstreamLifecycleMu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.releaseUDPSession("flow", session, true)
		}()
		<-reached
		e.resetDialContext()
		e.closeIdleUDPPool()
		e.closeActiveUpstreams()
		e.upstreamLifecycleMu.Unlock()
		<-done
		if !session.isClosed() || len(e.udpPool) != 0 {
			t.Fatalf("late udp release survived reset: closed=%v pool=%d", session.isClosed(), len(e.udpPool))
		}
	})
}

func cacheableDNSMessages() ([]byte, []byte) {
	req := []byte{
		0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x01, 'a', 0x00, 0x00, 0x01, 0x00, 0x01,
	}
	resp := append([]byte(nil), req...)
	resp[2], resp[3] = 0x81, 0x80
	resp[6], resp[7] = 0x00, 0x01
	resp = append(resp,
		0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x3c, 0x00, 0x04, 1, 1, 1, 1,
	)
	return req, resp
}

func TestEncodeSocks5UDPAddressIntoDoesNotAllocate(t *testing.T) {
	target := &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}
	buf := make([]byte, 19)
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := encodeSocks5UDPAddressInto(buf, target); err != nil {
			t.Fatalf("encode SOCKS5 UDP address: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("expected allocation-free address encoding, got %.2f allocs/op", allocs)
	}
}

func TestSocks5TLSUDPSessionConcurrentWritesPreserveFrames(t *testing.T) {
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen relay udp: %v", err)
	}
	defer relay.Close()

	client, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen client udp: %v", err)
	}
	defer client.Close()

	session := &socks5TLSUDPSession{
		udpConn:   client,
		relayAddr: relay.LocalAddr().(*net.UDPAddr),
		writeBuf:  make([]byte, 1),
	}

	const writes = 32
	errCh := make(chan error, writes)
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			target := &net.UDPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 10000 + index}
			errCh <- session.WriteTo([]byte{byte(index), 0xa5}, target)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent udp write: %v", err)
		}
	}

	if err := relay.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set relay deadline: %v", err)
	}
	seen := make(map[int]bool, writes)
	packet := make([]byte, 256)
	for i := 0; i < writes; i++ {
		n, _, err := relay.ReadFromUDP(packet)
		if err != nil {
			t.Fatalf("read relay frame %d: %v", i, err)
		}
		if n < 3 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
			t.Fatalf("invalid SOCKS5 UDP prefix: %x", packet[:n])
		}
		target, headerLen, err := decodeSocks5UDPAddress(packet[3:n])
		if err != nil {
			t.Fatalf("decode relay frame: %v", err)
		}
		body := packet[3+headerLen : n]
		if len(body) != 2 || body[1] != 0xa5 {
			t.Fatalf("corrupted relay payload: %x", body)
		}
		index := int(body[0])
		if index >= writes || target.Port != 10000+index || !target.IP.Equal(net.IPv4(8, 8, 8, 8)) {
			t.Fatalf("mismatched relay frame target=%v payload=%x", target, body)
		}
		if seen[index] {
			t.Fatalf("duplicate relay payload index %d", index)
		}
		seen[index] = true
	}
}
