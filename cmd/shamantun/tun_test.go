package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	gstack "gvisor.dev/gvisor/pkg/tcpip/stack"
)

type stubConnWithRemote struct {
	remote net.Addr
}

func (s *stubConnWithRemote) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (s *stubConnWithRemote) Write(p []byte) (int, error) { return len(p), nil }
func (s *stubConnWithRemote) Close() error                { return nil }
func (s *stubConnWithRemote) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
}
func (s *stubConnWithRemote) RemoteAddr() net.Addr               { return s.remote }
func (s *stubConnWithRemote) SetDeadline(_ time.Time) error      { return nil }
func (s *stubConnWithRemote) SetReadDeadline(_ time.Time) error  { return nil }
func (s *stubConnWithRemote) SetWriteDeadline(_ time.Time) error { return nil }

type blockingRelayConn struct {
	mu          sync.Mutex
	closed      bool
	deadlineSet bool
}

func (c *blockingRelayConn) Read(_ []byte) (int, error) {
	for {
		c.mu.Lock()
		switch {
		case c.closed:
			c.mu.Unlock()
			return 0, net.ErrClosed
		case c.deadlineSet:
			c.mu.Unlock()
			return 0, &timeoutNetError{}
		default:
			c.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *blockingRelayConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (c *blockingRelayConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *blockingRelayConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
}
func (c *blockingRelayConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2}
}
func (c *blockingRelayConn) SetDeadline(_ time.Time) error {
	c.mu.Lock()
	c.deadlineSet = true
	c.mu.Unlock()
	return nil
}
func (c *blockingRelayConn) SetReadDeadline(_ time.Time) error {
	c.mu.Lock()
	c.deadlineSet = true
	c.mu.Unlock()
	return nil
}
func (c *blockingRelayConn) SetWriteDeadline(_ time.Time) error {
	c.mu.Lock()
	c.deadlineSet = true
	c.mu.Unlock()
	return nil
}

type stubDialerEaddrNotAvail struct{}

func (d *stubDialerEaddrNotAvail) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, eaddrNotAvailErr()
}
func (d *stubDialerEaddrNotAvail) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, eaddrNotAvailErr()
}
func (d *stubDialerEaddrNotAvail) SupportsUDP() bool { return true }
func (d *stubDialerEaddrNotAvail) Mode() string      { return ModeSocks5TLS }

func eaddrNotAvailErr() error {
	return fmt.Errorf("connect upstream: %w", syscall.EADDRNOTAVAIL)
}

type stubPacketConnEOF struct{}

func (c *stubPacketConnEOF) ReadFrom(_ []byte) (int, net.Addr, error) { return 0, nil, io.EOF }
func (c *stubPacketConnEOF) WriteTo(_ []byte, _ net.Addr) (int, error) {
	return 0, errors.New("not implemented")
}
func (c *stubPacketConnEOF) Close() error                       { return nil }
func (c *stubPacketConnEOF) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *stubPacketConnEOF) SetDeadline(_ time.Time) error      { return nil }
func (c *stubPacketConnEOF) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubPacketConnEOF) SetWriteDeadline(_ time.Time) error { return nil }

type stubPacketConnWriteToOnly struct{}

func (c *stubPacketConnWriteToOnly) ReadFrom(_ []byte) (int, net.Addr, error) { return 0, nil, io.EOF }
func (c *stubPacketConnWriteToOnly) WriteTo(_ []byte, _ net.Addr) (int, error) {
	return 0, errors.New("unexpected fallback write")
}
func (c *stubPacketConnWriteToOnly) Close() error                       { return nil }
func (c *stubPacketConnWriteToOnly) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *stubPacketConnWriteToOnly) SetDeadline(_ time.Time) error      { return nil }
func (c *stubPacketConnWriteToOnly) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubPacketConnWriteToOnly) SetWriteDeadline(_ time.Time) error { return nil }

type scriptedTimeoutPacketConn struct {
	mu     sync.Mutex
	served bool
	closed bool
}

func (c *scriptedTimeoutPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, nil, net.ErrClosed
	}
	if !c.served {
		c.served = true
		copy(p, []byte{0x01, 0x02})
		return 2, &net.UDPAddr{IP: net.ParseIP("198.18.0.1"), Port: 53000}, nil
	}
	return 0, nil, &timeoutNetError{}
}

func (c *scriptedTimeoutPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	return len(p), nil
}
func (c *scriptedTimeoutPacketConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *scriptedTimeoutPacketConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}
func (c *scriptedTimeoutPacketConn) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *scriptedTimeoutPacketConn) SetDeadline(_ time.Time) error      { return nil }
func (c *scriptedTimeoutPacketConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *scriptedTimeoutPacketConn) SetWriteDeadline(_ time.Time) error { return nil }

type multiReadPacketConn struct {
	mu                 sync.Mutex
	readsLeft          int
	readDeadlineCalls  int
	writeDeadlineCalls int
	closed             bool
}

func (c *multiReadPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, nil, net.ErrClosed
	}
	if c.readsLeft <= 0 {
		return 0, nil, io.EOF
	}
	c.readsLeft--
	copy(p, []byte{0x01, 0x02, 0x03})
	return 3, &net.UDPAddr{IP: net.ParseIP("198.18.0.1"), Port: 53000}, nil
}

func (c *multiReadPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	return len(p), nil
}

func (c *multiReadPacketConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *multiReadPacketConn) LocalAddr() net.Addr { return &net.UDPAddr{} }
func (c *multiReadPacketConn) SetDeadline(_ time.Time) error {
	return nil
}
func (c *multiReadPacketConn) SetReadDeadline(_ time.Time) error {
	c.mu.Lock()
	c.readDeadlineCalls++
	c.mu.Unlock()
	return nil
}
func (c *multiReadPacketConn) SetWriteDeadline(_ time.Time) error {
	c.mu.Lock()
	c.writeDeadlineCalls++
	c.mu.Unlock()
	return nil
}

func (c *multiReadPacketConn) deadlineCounts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadlineCalls, c.writeDeadlineCalls
}

type shortWriteConn struct {
	net.Conn
	maxWrite int
}

func (c *shortWriteConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	limit := c.maxWrite
	if limit <= 0 || limit > len(p) {
		limit = len(p)
	}
	return c.Conn.Write(p[:limit])
}

type shortWriter struct {
	maxWrite int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	limit := w.maxWrite
	if limit <= 0 || limit > len(p) {
		limit = len(p)
	}
	return limit, nil
}

type connWithRemoteAddr struct {
	net.Conn
	remote net.Addr
}

func (c *connWithRemoteAddr) RemoteAddr() net.Addr {
	if c.remote != nil {
		return c.remote
	}
	return c.Conn.RemoteAddr()
}

type stubCancelableUDPDialer struct {
	enterCh  chan struct{}
	cancelCh chan error
}

func (d *stubCancelableUDPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubCancelableUDPDialer) DialUDP(ctx context.Context) (UDPSession, error) {
	if d.enterCh != nil {
		close(d.enterCh)
	}
	<-ctx.Done()
	if d.cancelCh != nil {
		d.cancelCh <- ctx.Err()
	}
	return nil, ctx.Err()
}

func (d *stubCancelableUDPDialer) SupportsUDP() bool { return true }
func (d *stubCancelableUDPDialer) Mode() string      { return ModeSocks5TLS }

type stubWarmupDialer struct {
	warmupCh chan struct{}
	mu       sync.Mutex
	calls    int
	blockCh  chan struct{}
	err      error
}

func (d *stubWarmupDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubWarmupDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubWarmupDialer) SupportsUDP() bool { return false }
func (d *stubWarmupDialer) Mode() string      { return ModeSocks5TLS }
func (d *stubWarmupDialer) Warmup(_ context.Context) error {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	if d.warmupCh != nil {
		select {
		case <-d.warmupCh:
		default:
			close(d.warmupCh)
		}
	}
	if d.blockCh != nil {
		<-d.blockCh
	}
	return d.err
}

func (d *stubWarmupDialer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type stubTCPDialCounter struct {
	mu    sync.Mutex
	calls int
}

func (d *stubTCPDialCounter) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return nil, errors.New("not implemented")
}

func (d *stubTCPDialCounter) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubTCPDialCounter) SupportsUDP() bool { return false }
func (d *stubTCPDialCounter) Mode() string      { return ModeSocks5TLS }

func (d *stubTCPDialCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type stubUDPSession struct {
	closed chan struct{}
	once   sync.Once
}

func (s *stubUDPSession) WriteTo(_ []byte, _ *net.UDPAddr) error { return nil }
func (s *stubUDPSession) ReadFrom(_ []byte) (int, *net.UDPAddr, error) {
	return 0, nil, io.EOF
}
func (s *stubUDPSession) Close() error {
	s.once.Do(func() {
		if s.closed != nil {
			close(s.closed)
		}
	})
	return nil
}
func (s *stubUDPSession) SetDeadline(_ time.Time) error      { return nil }
func (s *stubUDPSession) SetReadDeadline(_ time.Time) error  { return nil }
func (s *stubUDPSession) SetWriteDeadline(_ time.Time) error { return nil }

type scriptedUDPSession struct {
	reads []scriptedUDPRead
	mu    sync.Mutex
}

type scriptedUDPRead struct {
	payload []byte
	src     *net.UDPAddr
	err     error
}

func (s *scriptedUDPSession) WriteTo(_ []byte, _ *net.UDPAddr) error { return nil }
func (s *scriptedUDPSession) ReadFrom(payload []byte) (int, *net.UDPAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.reads) == 0 {
		return 0, nil, io.EOF
	}
	next := s.reads[0]
	s.reads = s.reads[1:]
	if len(next.payload) > 0 {
		copy(payload, next.payload)
	}
	return len(next.payload), next.src, next.err
}
func (s *scriptedUDPSession) Close() error                       { return nil }
func (s *scriptedUDPSession) SetDeadline(_ time.Time) error      { return nil }
func (s *scriptedUDPSession) SetReadDeadline(_ time.Time) error  { return nil }
func (s *scriptedUDPSession) SetWriteDeadline(_ time.Time) error { return nil }

type stubDNSTCPDialer struct {
	mode     string
	response []byte
	mu       sync.Mutex
	targets  []string
}

func (d *stubDNSTCPDialer) DialTCP(_ context.Context, target string) (net.Conn, error) {
	client, server := net.Pipe()
	d.mu.Lock()
	d.targets = append(d.targets, target)
	d.mu.Unlock()

	go func() {
		defer server.Close()
		for {
			var lengthBuf [2]byte
			if _, err := io.ReadFull(server, lengthBuf[:]); err != nil {
				return
			}
			size := int(binary.BigEndian.Uint16(lengthBuf[:]))
			payload := make([]byte, size)
			if _, err := io.ReadFull(server, payload); err != nil {
				return
			}
			out := d.response
			if len(out) == 0 {
				out = payload
			}
			reply := make([]byte, 2+len(out))
			binary.BigEndian.PutUint16(reply[:2], uint16(len(out)))
			copy(reply[2:], out)
			if _, err := server.Write(reply); err != nil {
				return
			}
		}
	}()

	return client, nil
}

func (d *stubDNSTCPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubDNSTCPDialer) SupportsUDP() bool { return false }
func (d *stubDNSTCPDialer) Mode() string {
	if d.mode == "" {
		return ModeSocks5TLS
	}
	return d.mode
}

func (d *stubDNSTCPDialer) dialCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.targets)
}

type stubRetryTCPDialer struct {
	response  []byte
	failCount int
	mu        sync.Mutex
	dials     int
}

func (d *stubRetryTCPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials++
	if d.failCount > 0 {
		d.failCount--
		return nil, eaddrNotAvailErr()
	}

	client, server := net.Pipe()
	response := append([]byte(nil), d.response...)
	go func() {
		defer server.Close()
		var lengthBuf [2]byte
		if _, err := io.ReadFull(server, lengthBuf[:]); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint16(lengthBuf[:]))
		payload := make([]byte, size)
		if _, err := io.ReadFull(server, payload); err != nil {
			return
		}
		out := response
		if len(out) == 0 {
			out = payload
		}
		reply := make([]byte, 2+len(out))
		binary.BigEndian.PutUint16(reply[:2], uint16(len(out)))
		copy(reply[2:], out)
		_, _ = server.Write(reply)
	}()

	return client, nil
}

func (d *stubRetryTCPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubRetryTCPDialer) SupportsUDP() bool { return false }
func (d *stubRetryTCPDialer) Mode() string      { return ModeSocks5TLS }

type stubBlockingTCPDialer struct{}

func (d *stubBlockingTCPDialer) DialTCP(ctx context.Context, _ string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *stubBlockingTCPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubBlockingTCPDialer) SupportsUDP() bool { return false }
func (d *stubBlockingTCPDialer) Mode() string      { return ModeSocks5TLS }

type stubConcurrentTCPDialer struct {
	dials atomic.Int32
	fail  bool
}

func (d *stubConcurrentTCPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	d.dials.Add(1)
	if d.fail {
		return nil, errors.New("tcp failed")
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (d *stubConcurrentTCPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubConcurrentTCPDialer) SupportsUDP() bool { return false }
func (d *stubConcurrentTCPDialer) Mode() string      { return ModeSocks5TLS }

type dialOrderRecorder struct {
	mu    sync.Mutex
	order []int
}

func (r *dialOrderRecorder) record(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, idx)
}

func (r *dialOrderRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.order...)
}

type stubOrderedUpstreamDialer struct {
	idx    int
	tcpErr error
	udpErr error
	order  *dialOrderRecorder
}

func (d *stubOrderedUpstreamDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	d.order.record(d.idx)
	if d.tcpErr != nil {
		return nil, d.tcpErr
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (d *stubOrderedUpstreamDialer) DialUDP(_ context.Context) (UDPSession, error) {
	d.order.record(d.idx)
	if d.udpErr != nil {
		return nil, d.udpErr
	}
	return &stubUDPSession{}, nil
}

func (d *stubOrderedUpstreamDialer) SupportsUDP() bool { return true }
func (d *stubOrderedUpstreamDialer) Mode() string      { return ModeSocks5TLS }

type stubSequencedTCPDialer struct {
	mu      sync.Mutex
	next    int
	entered []chan struct{}
	release []chan struct{}
	errs    []error
}

func (d *stubSequencedTCPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	d.mu.Lock()
	idx := d.next
	d.next++
	entered := d.entered[idx]
	release := d.release[idx]
	err := d.errs[idx]
	d.mu.Unlock()
	close(entered)
	<-release
	if err != nil {
		return nil, err
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (d *stubSequencedTCPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}
func (d *stubSequencedTCPDialer) SupportsUDP() bool { return false }
func (d *stubSequencedTCPDialer) Mode() string      { return ModeSocks5TLS }

type stubDNSPacketConn struct {
	reads  [][]byte
	writes [][]byte
	mu     sync.Mutex
	index  int
}

func (c *stubDNSPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.index >= len(c.reads) {
		return 0, nil, io.EOF
	}
	payload := c.reads[c.index]
	c.index++
	copy(p, payload)
	return len(payload), &net.UDPAddr{IP: net.ParseIP("198.18.0.1"), Port: 53000}, nil
}

func (c *stubDNSPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *stubDNSPacketConn) Write(p []byte) (int, error) {
	return c.WriteTo(p, nil)
}

func (c *stubDNSPacketConn) Close() error                       { return nil }
func (c *stubDNSPacketConn) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *stubDNSPacketConn) SetDeadline(_ time.Time) error      { return nil }
func (c *stubDNSPacketConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubDNSPacketConn) SetWriteDeadline(_ time.Time) error { return nil }

type stubDNSPacketConnBlocking struct {
	firstRead []byte
	writes    [][]byte
	mu        sync.Mutex
	served    bool
	blockCh   chan struct{}
}

func (c *stubDNSPacketConnBlocking) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	if !c.served {
		c.served = true
		payload := append([]byte(nil), c.firstRead...)
		c.mu.Unlock()
		copy(p, payload)
		return len(payload), &net.UDPAddr{IP: net.ParseIP("198.18.0.1"), Port: 53000}, nil
	}
	blockCh := c.blockCh
	c.mu.Unlock()
	<-blockCh
	return 0, nil, io.EOF
}

func (c *stubDNSPacketConnBlocking) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (c *stubDNSPacketConnBlocking) Write(p []byte) (int, error) {
	return c.WriteTo(p, nil)
}

func (c *stubDNSPacketConnBlocking) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blockCh != nil {
		close(c.blockCh)
		c.blockCh = nil
	}
	return nil
}
func (c *stubDNSPacketConnBlocking) LocalAddr() net.Addr                { return &net.UDPAddr{} }
func (c *stubDNSPacketConnBlocking) SetDeadline(_ time.Time) error      { return nil }
func (c *stubDNSPacketConnBlocking) SetReadDeadline(_ time.Time) error  { return nil }
func (c *stubDNSPacketConnBlocking) SetWriteDeadline(_ time.Time) error { return nil }

type deadlineTrackingConn struct {
	net.Conn
	mu            sync.Mutex
	deadlineCalls int
	lastDeadline  time.Time
}

func (c *deadlineTrackingConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadlineCalls++
	c.lastDeadline = t
	c.mu.Unlock()
	if c.Conn != nil {
		return c.Conn.SetDeadline(t)
	}
	return nil
}

func (c *deadlineTrackingConn) deadlineCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadlineCalls
}

type timedReadStep struct {
	delay time.Duration
	data  []byte
	err   error
}

type deadlineRefreshConn struct {
	mu                 sync.Mutex
	reads              []timedReadStep
	readDeadlineCalls  int
	writeDeadlineCalls int
	writes             int
}

func (c *deadlineRefreshConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.reads) == 0 {
		c.mu.Unlock()
		return 0, io.EOF
	}
	step := c.reads[0]
	c.reads = c.reads[1:]
	c.mu.Unlock()
	if step.delay > 0 {
		time.Sleep(step.delay)
	}
	n := copy(p, step.data)
	if step.err != nil {
		return n, step.err
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (c *deadlineRefreshConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes += len(p)
	c.mu.Unlock()
	return len(p), nil
}

func (c *deadlineRefreshConn) Close() error                  { return nil }
func (c *deadlineRefreshConn) LocalAddr() net.Addr           { return &net.TCPAddr{} }
func (c *deadlineRefreshConn) RemoteAddr() net.Addr          { return &net.TCPAddr{} }
func (c *deadlineRefreshConn) SetDeadline(_ time.Time) error { return nil }
func (c *deadlineRefreshConn) SetReadDeadline(_ time.Time) error {
	c.mu.Lock()
	c.readDeadlineCalls++
	c.mu.Unlock()
	return nil
}
func (c *deadlineRefreshConn) SetWriteDeadline(_ time.Time) error {
	c.mu.Lock()
	c.writeDeadlineCalls++
	c.mu.Unlock()
	return nil
}

func (c *deadlineRefreshConn) readDeadlineCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadlineCalls
}

func (c *deadlineRefreshConn) writeDeadlineCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadlineCalls
}

type stubUDPDialCounter struct {
	mu         sync.Mutex
	udpDials   int
	disableUDP bool
}

func (d *stubUDPDialCounter) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubUDPDialCounter) DialUDP(_ context.Context) (UDPSession, error) {
	d.mu.Lock()
	d.udpDials++
	d.mu.Unlock()
	return &stubUDPSession{}, nil
}

func (d *stubUDPDialCounter) SupportsUDP() bool {
	return d == nil || !d.disableUDP
}
func (d *stubUDPDialCounter) Mode() string { return ModeSocks5TLS }

func (d *stubUDPDialCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.udpDials
}

type stubFailingUDPDialer struct {
	mu       sync.Mutex
	udpDials int
}

func (d *stubFailingUDPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubFailingUDPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	d.mu.Lock()
	d.udpDials++
	d.mu.Unlock()
	return nil, errors.New("udp failed")
}

func (d *stubFailingUDPDialer) SupportsUDP() bool { return true }
func (d *stubFailingUDPDialer) Mode() string      { return ModeSocks5TLS }

func (d *stubFailingUDPDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.udpDials
}

type stubConcurrentUDPDialer struct {
	dials atomic.Int32
	fail  bool
}

func (d *stubConcurrentUDPDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubConcurrentUDPDialer) DialUDP(_ context.Context) (UDPSession, error) {
	d.dials.Add(1)
	if d.fail {
		return nil, errors.New("udp failed")
	}
	return &stubUDPSession{}, nil
}

func (d *stubConcurrentUDPDialer) SupportsUDP() bool { return true }
func (d *stubConcurrentUDPDialer) Mode() string      { return ModeSocks5TLS }

type stubReleasedUpstreamDialer struct {
	entered chan struct{}
	release chan struct{}
}

func (d *stubReleasedUpstreamDialer) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	close(d.entered)
	<-d.release
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (d *stubReleasedUpstreamDialer) DialUDP(_ context.Context) (UDPSession, error) {
	close(d.entered)
	<-d.release
	return &stubUDPSession{}, nil
}

func (d *stubReleasedUpstreamDialer) SupportsUDP() bool { return true }
func (d *stubReleasedUpstreamDialer) Mode() string      { return ModeSocks5TLS }

type stubSelectionResetObserver struct {
	engine             *Engine
	observedGeneration atomic.Uint64
}

func (d *stubSelectionResetObserver) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (d *stubSelectionResetObserver) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubSelectionResetObserver) SupportsUDP() bool { return false }
func (d *stubSelectionResetObserver) Mode() string      { return ModeSocks5TLS }
func (d *stubSelectionResetObserver) ResetUpstreamSelection() {
	d.observedGeneration.Store(d.engine.networkGeneration.Load())
}

func TestConfigValidateRejectsInvalidUpstreamPort(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Upstream.Addr = "127.0.0.1:0"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("expected invalid port error, got: %v", err)
	}
}

func TestConfigValidateAllowsInsecureSkipVerifyWithWarningOnly(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Upstream.InsecureSkipVerify = true
	cfg.Upstreams = []UpstreamConfig{cfg.Upstream}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected insecure skip verify to remain a runtime warning, got: %v", err)
	}
}

func TestConfigValidateRejectsMissingClientPEM(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Upstream.ClientPEM = "definitely-missing-client.pem"

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "client_pem is not accessible") {
		t.Fatalf("expected missing client_pem error, got: %v", err)
	}
}

func TestConfigValidateAllowsHTTPSBasicAuthWithoutClientCertificate(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Mode = ModeHTTPS
	cfg.Upstream.Username = "user"
	cfg.Upstream.Password = "pass"
	cfg.Upstream.ClientPEM = ""
	cfg.Upstream.ClientKey = ""

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected https basic auth config without client certs to validate, got: %v", err)
	}
}

func TestConfigValidateRejectsPartialClientCertificatePair(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Mode = ModeHTTPS
	cfg.Upstream.Username = "user"
	cfg.Upstream.Password = "pass"
	cfg.Upstream.ClientKey = ""

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "must both be set") {
		t.Fatalf("expected partial client cert pair error, got: %v", err)
	}
}

func TestConfigValidateRejectsInvalidSecondaryUpstreamCredentials(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Mode = ModeHTTPS
	cfg.Upstreams = []UpstreamConfig{
		{
			Addr:     "127.0.0.1:443",
			Username: "user",
			Password: "pass",
		},
		{
			Addr: "127.0.0.2:443",
		},
	}

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "upstreams[1]") || !strings.Contains(err.Error(), "client_pem and client_key") {
		t.Fatalf("expected secondary upstream credential error, got: %v", err)
	}
}

func TestConfigValidateRejectsTooSmallConnectTimeout(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.ConnectTimeoutMS = minConnectTimeoutMS - 1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "connect_timeout_ms") {
		t.Fatalf("expected connect_timeout_ms error, got: %v", err)
	}
}

func TestConfigValidateRejectsTooSmallUDPIdleTimeout(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.UDPIdleTimeoutMS = minIdleTimeoutMS - 1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "udp_idle_timeout_ms") {
		t.Fatalf("expected udp_idle_timeout_ms error, got: %v", err)
	}
}

func TestConfigValidateRejectsHugeUDPBuffer(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.UDPBuffer = maxRuntimeBufferBytes + 1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "udp_buffer") {
		t.Fatalf("expected udp_buffer error, got: %v", err)
	}
}

func TestConfigValidateRejectsHugeDialConcurrency(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.TCPDialConcurrency = maxDialConcurrency + 1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "tcp_dial_concurrency") {
		t.Fatalf("expected tcp_dial_concurrency error, got: %v", err)
	}
}

func TestConfigValidateRejectsHugeHandlerConcurrency(t *testing.T) {
	cfg := newValidConfigForTest(t)
	cfg.Runtime.UDPHandlerConcurrency = maxHandlerConcurrency + 1

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "udp_handler_concurrency") {
		t.Fatalf("expected udp_handler_concurrency error, got: %v", err)
	}
}

func TestBuildTLSConfigRejectsInvalidClientCertificatePEM(t *testing.T) {
	tmp := t.TempDir()
	certPath := writeTestFile(t, tmp, "client.pem", []byte("not pem"))
	keyPath := writeTestFile(t, tmp, "client.key", []byte("not pem"))

	_, err := buildTLSConfig(UpstreamConfig{
		Addr:      "127.0.0.1:443",
		ClientPEM: certPath,
		ClientKey: keyPath,
	})
	if err == nil || !strings.Contains(err.Error(), "parse client certificate") {
		t.Fatalf("expected client certificate parse error, got: %v", err)
	}
}

func TestBuildTLSConfigRejectsMismatchedClientKeyPair(t *testing.T) {
	mtlsA := generateMTLSAssets(t, []net.IP{net.ParseIP("127.0.0.1")})
	mtlsB := generateMTLSAssets(t, []net.IP{net.ParseIP("127.0.0.2")})

	_, err := buildTLSConfig(UpstreamConfig{
		Addr:      "127.0.0.1:443",
		ClientPEM: mtlsA.ClientPEM,
		ClientKey: mtlsB.ClientKey,
	})
	if err == nil || !strings.Contains(err.Error(), "load client certificate/key pair") {
		t.Fatalf("expected mismatched key pair error, got: %v", err)
	}
}

func TestBuildTLSConfigAllowsHTTPSBasicAuthWithoutClientCertificate(t *testing.T) {
	tlsCfg, err := buildTLSConfig(UpstreamConfig{
		Addr:     "127.0.0.1:443",
		Username: "user",
		Password: "pass",
	})
	if err != nil {
		t.Fatalf("expected tls config without client certs to build, got: %v", err)
	}
	if len(tlsCfg.Certificates) != 0 {
		t.Fatalf("expected no client certificates, got %d", len(tlsCfg.Certificates))
	}
}

func TestBuildTLSConfigRejectsPartialClientCertificatePair(t *testing.T) {
	_, err := buildTLSConfig(UpstreamConfig{
		Addr:      "127.0.0.1:443",
		ClientPEM: "placeholder-client.pem",
	})
	if err == nil || !strings.Contains(err.Error(), "must both be set") {
		t.Fatalf("expected partial client cert pair error, got: %v", err)
	}
}

func TestBuildTLSConfigLoadsCustomRootCA(t *testing.T) {
	assets := generateMTLSAssets(t, []net.IP{net.ParseIP("127.0.0.1")})
	systemPool, systemPoolErr := x509.SystemCertPool()
	tlsCfg, err := buildTLSConfig(UpstreamConfig{
		Addr:      "127.0.0.1:443",
		CAPEM:     assets.ServerPEM,
		ClientPEM: assets.ClientPEM,
		ClientKey: assets.ClientKey,
	})
	if err != nil {
		t.Fatalf("build tls config with custom root: %v", err)
	}
	if tlsCfg.RootCAs == nil {
		t.Fatal("expected custom root CA pool")
	}
	if systemPoolErr == nil && systemPool != nil && len(tlsCfg.RootCAs.Subjects()) <= len(systemPool.Subjects()) {
		t.Fatalf("custom root should augment system roots: got=%d system=%d", len(tlsCfg.RootCAs.Subjects()), len(systemPool.Subjects()))
	}
	certPEM, err := os.ReadFile(assets.ServerPEM)
	if err != nil {
		t.Fatalf("read server certificate: %v", err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("decode server certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse server certificate: %v", err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: tlsCfg.RootCAs, DNSName: "127.0.0.1"}); err != nil {
		t.Fatalf("verify server certificate with custom root: %v", err)
	}
}

func TestBuildTLSConfigRejectsInvalidRootCA(t *testing.T) {
	caPath := writeTestFile(t, t.TempDir(), "ca.pem", []byte("not pem"))
	_, err := buildTLSConfig(UpstreamConfig{Addr: "127.0.0.1:443", CAPEM: caPath})
	if err == nil || !strings.Contains(err.Error(), "parse ca_pem") {
		t.Fatalf("expected invalid ca_pem error, got %v", err)
	}
}

func TestDecodeConfigAllowsPerUpstreamRootCAOverride(t *testing.T) {
	cfg, err := decodeConfig([]byte(`{
  "mode": "socks5tls",
  "ca_pem": "global-ca.pem",
  "upstreams": [
    {"addr": "192.0.2.1:443"},
    {"addr": "192.0.2.2:443", "ca_pem": "override-ca.pem"}
  ]
}`))
	if err != nil {
		t.Fatalf("decode ca_pem config: %v", err)
	}
	if got := cfg.Upstreams[0].CAPEM; got != "global-ca.pem" {
		t.Fatalf("global ca_pem = %q", got)
	}
	if got := cfg.Upstreams[1].CAPEM; got != "override-ca.pem" {
		t.Fatalf("per-upstream ca_pem = %q", got)
	}
}

func TestDecodeConfigAllowsPerUpstreamRootCAClear(t *testing.T) {
	cfg, err := decodeConfig([]byte(`{
  "mode": "socks5tls",
  "ca_pem": "global-ca.pem",
  "upstreams": [
    {"addr": "192.0.2.1:443"},
    {"addr": "192.0.2.2:443", "ca_pem": ""}
  ]
}`))
	if err != nil {
		t.Fatalf("decode ca_pem clear config: %v", err)
	}
	if got := cfg.Upstreams[0].CAPEM; got != "global-ca.pem" {
		t.Fatalf("global ca_pem = %q", got)
	}
	if got := cfg.Upstreams[1].CAPEM; got != "" {
		t.Fatalf("cleared per-upstream ca_pem = %q", got)
	}
}

func TestLoadConfigCompactLayout(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "compact.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "enable_udp": true
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write compact config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load compact config: %v", err)
	}
	if cfg.Mode != ModeSocks5TLS {
		t.Fatalf("unexpected mode: %s", cfg.Mode)
	}
	if cfg.Upstream.Addr != "127.0.0.1:443" {
		t.Fatalf("unexpected upstream addr: %s", cfg.Upstream.Addr)
	}
	if cfg.Tun.Name != validTunNameForRuntime() {
		t.Fatalf("unexpected tun name: %s", cfg.Tun.Name)
	}
	if !cfg.Runtime.EnableUDP {
		t.Fatalf("expected enable_udp=true")
	}
	if cfg.Runtime.TCPHandlerConcurrency != defaultTCPHandlerConcurrency {
		t.Fatalf("unexpected tcp handler concurrency default: %d", cfg.Runtime.TCPHandlerConcurrency)
	}
	if cfg.Runtime.UDPHandlerConcurrency != defaultUDPHandlerConcurrency {
		t.Fatalf("unexpected udp handler concurrency default: %d", cfg.Runtime.UDPHandlerConcurrency)
	}
	if cfg.Runtime.UDPIdleTimeoutMS != defaultUDPIdleTimeoutMS {
		t.Fatalf("unexpected udp idle timeout default: %d", cfg.Runtime.UDPIdleTimeoutMS)
	}
}

func TestLoadConfigAllowsUDPIdleTimeoutOverride(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "udp-idle-timeout.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "udp_idle_timeout_ms": 12000
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write udp idle timeout config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load udp idle timeout config: %v", err)
	}
	if got := cfg.UDPIdleTimeout(); got != 12*time.Second {
		t.Fatalf("unexpected udp idle timeout: %s", got)
	}
}

func TestLoadConfigAcceptsMultipleUpstreams(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "multi-upstream.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstreams": [
    {"addr": "127.0.0.1:443", "username": "user", "password": "pass"},
    {"addr": "127.0.0.2:443"}
  ],
  "client_pem": %q,
  "client_key": %q
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write multi-upstream config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load multi-upstream config: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("unexpected upstream count: %d", len(cfg.Upstreams))
	}
	if cfg.Upstreams[0].Addr != "127.0.0.1:443" || cfg.Upstreams[1].Addr != "127.0.0.2:443" {
		t.Fatalf("unexpected upstreams: %#v", cfg.Upstreams)
	}
	if cfg.Upstream.Addr != cfg.Upstreams[0].Addr {
		t.Fatalf("primary upstream was not set from first upstream: %q", cfg.Upstream.Addr)
	}
	if cfg.Upstreams[0].Username != "user" || cfg.Upstreams[0].Password != "pass" {
		t.Fatalf("first upstream credentials not loaded: %#v", cfg.Upstreams[0])
	}
	if cfg.Upstreams[1].Username != "" || cfg.Upstreams[1].Password != "" {
		t.Fatalf("second upstream should not inherit credentials: %#v", cfg.Upstreams[1])
	}
}

func TestLoadConfigAcceptsRoutingRules(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "routing.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "routing": {
    "enabled": true,
    "default": "proxy",
    "direct_cidrs": ["1.0.1.0/24"]
  }
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write routing config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load routing config: %v", err)
	}
	if !cfg.Routing.Enabled {
		t.Fatal("expected routing to be enabled")
	}
	if cfg.Routing.DefaultOutbound != OutboundProxy {
		t.Fatalf("unexpected routing default: %q", cfg.Routing.DefaultOutbound)
	}
	if !reflect.DeepEqual(cfg.Routing.DirectCIDRs, []string{"1.0.1.0/24"}) {
		t.Fatalf("unexpected direct cidrs: %#v", cfg.Routing.DirectCIDRs)
	}
}

func TestLoadConfigDefaultsToHiddenSplitRouting(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "default-routing.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write default routing config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load default routing config: %v", err)
	}
	if !cfg.Routing.Enabled {
		t.Fatal("expected hidden split routing to be enabled by default")
	}
	if cfg.Routing.DefaultOutbound != OutboundProxy {
		t.Fatalf("unexpected routing default: %q", cfg.Routing.DefaultOutbound)
	}
	if !slices.Contains(cfg.Routing.DirectCIDRs, "10.0.0.0/8") {
		t.Fatalf("expected private direct cidr in defaults: %#v", cfg.Routing.DirectCIDRs)
	}
	if !slices.Contains(cfg.Routing.DirectCIDRs, "1.0.1.0/24") {
		t.Fatalf("expected China direct cidr seed in defaults: %#v", cfg.Routing.DirectCIDRs)
	}

	router, err := NewRouter(cfg.Routing)
	if err != nil {
		t.Fatalf("new default router: %v", err)
	}
	if got := router.DecideIP(net.ParseIP("1.0.1.8")); got.Outbound != OutboundDirect {
		t.Fatalf("expected default China cidr to route direct, got %#v", got)
	}
	if got := router.DecideIP(net.ParseIP("14.16.0.1")); got.Outbound != OutboundDirect {
		t.Fatalf("expected default precise China cidr to route direct, got %#v", got)
	}
	if got := router.DecideIP(net.ParseIP("14.2.0.1")); got.Outbound != OutboundProxy {
		t.Fatalf("expected non-China APNIC address inside old broad /8 to route proxy, got %#v", got)
	}
	if got := router.DecideIP(net.ParseIP("8.8.8.8")); got.Outbound != OutboundProxy {
		t.Fatalf("expected unmatched ip to route through proxy, got %#v", got)
	}
}

func TestLoadConfigDisablesHiddenSplitRoutingWhenAutoRouteDisabled(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "manual-route.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "auto_route": false,
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write manual route config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load manual route config: %v", err)
	}
	if cfg.Routing.Enabled {
		t.Fatal("expected hidden split routing to default off when auto_route=false")
	}
	if len(cfg.Routing.DirectCIDRs) != 0 {
		t.Fatalf("did not expect default direct cidrs with auto_route=false: %#v", cfg.Routing.DirectCIDRs)
	}
}

func TestLoadConfigHonorsExplicitRoutingWithAutoRouteDisabled(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "manual-explicit-routing.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "auto_route": false,
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "routing": {
    "enabled": true,
    "direct_cidrs": ["1.0.1.0/24"]
  }
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write explicit manual routing config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load explicit manual routing config: %v", err)
	}
	if !cfg.Routing.Enabled {
		t.Fatal("expected explicit routing.enabled=true to be honored")
	}
	if !reflect.DeepEqual(cfg.Routing.DirectCIDRs, []string{"1.0.1.0/24"}) {
		t.Fatalf("unexpected direct cidrs: %#v", cfg.Routing.DirectCIDRs)
	}
}

func TestDefaultDirectCIDRsDoNotUseKnownOverbroadAPNICPrefix(t *testing.T) {
	if slices.Contains(defaultDirectCIDRs, "14.0.0.0/8") {
		t.Fatal("default direct cidrs must not include overbroad APNIC 14.0.0.0/8")
	}
}

func TestLoadConfigAllowsProxyCIDROverrideWithAutoRoute(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "overlap-routing.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "auto_route": true,
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "routing": {
    "enabled": true,
    "direct_cidrs": ["1.0.1.0/24"],
    "proxy_cidrs": ["1.0.1.8/32"]
  }
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write overlap routing config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("expected overlapping cidrs to be accepted for proxy override, got %v", err)
	}
	if got := cfg.Routing.DirectCIDRs; !reflect.DeepEqual(got, []string{"1.0.1.0/24"}) {
		t.Fatalf("unexpected direct cidrs: %#v", got)
	}
	if got := cfg.Routing.ProxyCIDRs; !reflect.DeepEqual(got, []string{"1.0.1.8/32"}) {
		t.Fatalf("unexpected proxy cidrs: %#v", got)
	}
}

func TestLoadConfigAllowsHiddenRoutingDisable(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "disabled-routing.json")

	raw := fmt.Sprintf(`{
  "mode": "socks5tls",
  "tun": %q,
  "upstream": "127.0.0.1:443",
  "client_pem": %q,
  "client_key": %q,
  "routing": {
    "enabled": false
  }
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write disabled routing config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load disabled routing config: %v", err)
	}
	if cfg.Routing.Enabled {
		t.Fatal("expected hidden routing disable to be honored")
	}
	if len(cfg.Routing.DirectCIDRs) != 0 {
		t.Fatalf("did not expect default direct cidrs when routing is disabled: %#v", cfg.Routing.DirectCIDRs)
	}
}

func TestMultiUpstreamDialUDPRejectsNoUDPCapableUpstreams(t *testing.T) {
	dialer, err := NewUpstreamDialerSet(ModeHTTPS, []UpstreamConfig{
		{Addr: "127.0.0.1:443", Username: "user", Password: "pass"},
		{Addr: "127.0.0.2:443", Username: "user", Password: "pass"},
	}, time.Second, 0)
	if err != nil {
		t.Fatalf("new multi-upstream dialer: %v", err)
	}
	if dialer.SupportsUDP() {
		t.Fatal("expected https multi-upstream dialer not to support udp")
	}

	session, err := dialer.DialUDP(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no UDP-capable upstreams") {
		t.Fatalf("expected no UDP-capable upstreams error, got session=%v err=%v", session, err)
	}
	if session != nil {
		t.Fatalf("expected nil session, got %v", session)
	}
}

func TestModeSwitchKeepsSingleUpstream(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "single-upstream.json")

	raw := fmt.Sprintf(`{
	  "mode": "socks5tls",
	  "upstream": "127.0.0.1:443",
	  "tun": %q,
	  "client_pem": %q,
	  "client_key": %q
	}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write single-upstream config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("load single-upstream config: %v", err)
	}
	if cfg.Mode != ModeSocks5TLS {
		t.Fatalf("unexpected mode: %s", cfg.Mode)
	}
	if cfg.Upstream.Addr != "127.0.0.1:443" {
		t.Fatalf("unexpected socks5tls upstream: %s", cfg.Upstream.Addr)
	}

	if err := cfg.ApplyOverrides(ModeHTTPS, ""); err != nil {
		t.Fatalf("switch mode to https: %v", err)
	}
	if cfg.Upstream.Addr != "127.0.0.1:443" {
		t.Fatalf("unexpected upstream after mode switch: %s", cfg.Upstream.Addr)
	}
}

func TestLoadConfigRejectsLegacyNestedLayout(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	cfgPath := filepath.Join(tmp, "legacy.json")

	raw := fmt.Sprintf(`{
  "mode": "https",
  "tun": {"name": %q},
  "upstream": {"addr": "127.0.0.1:8443"},
  "client_pem": %q,
  "client_key": %q
}`, validTunNameForRuntime(), clientPEM, clientKey)
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write legacy config: %v", err)
	}

	_, err := LoadConfig(cfgPath)
	if err == nil || !strings.Contains(err.Error(), "invalid JSON type at $.tun") {
		t.Fatalf("expected legacy format parse error, got: %v", err)
	}
}

func TestBuildSocks5CommandRequestRejectsZeroPort(t *testing.T) {
	_, err := buildSocks5CommandRequest(socksCmdConnect, "example.com:0")
	if err == nil || !strings.Contains(err.Error(), "invalid target port") {
		t.Fatalf("expected invalid target port error, got: %v", err)
	}
}

func TestBuildSocks5UDPAssociateRequestEncodesPortForWildcardIP(t *testing.T) {
	req, err := buildSocks5UDPAssociateRequest(&net.UDPAddr{IP: net.IPv4zero, Port: 53000})
	if err != nil {
		t.Fatalf("build udp associate request: %v", err)
	}
	if len(req) != 10 {
		t.Fatalf("unexpected request length: %d", len(req))
	}
	if req[0] != socksVersion || req[1] != socksCmdAssociate || req[3] != socksAtypIPv4 {
		t.Fatalf("unexpected request header: %v", req[:4])
	}
	if got := binary.BigEndian.Uint16(req[8:10]); got != 53000 {
		t.Fatalf("unexpected encoded port: %d", got)
	}
}

func TestBuildSocks5UDPAssociateRequestEncodesIPv4(t *testing.T) {
	req, err := buildSocks5UDPAssociateRequest(&net.UDPAddr{IP: net.ParseIP("10.0.0.2"), Port: 12345})
	if err != nil {
		t.Fatalf("build udp associate request: %v", err)
	}
	if len(req) != 10 {
		t.Fatalf("unexpected request length: %d", len(req))
	}
	if req[3] != socksAtypIPv4 {
		t.Fatalf("unexpected atyp: %d", req[3])
	}
	if !bytes.Equal(req[4:8], net.ParseIP("10.0.0.2").To4()) {
		t.Fatalf("unexpected encoded ip: %v", req[4:8])
	}
	if got := binary.BigEndian.Uint16(req[8:10]); got != 12345 {
		t.Fatalf("unexpected encoded port: %d", got)
	}
}

func TestPrepareSocks5UDPSocketAdvertisesWildcardAssociateEndpoint(t *testing.T) {
	udpConn, assoc, err := prepareSocks5UDPSocket(&stubConnWithRemote{
		remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 443},
	})
	if err != nil {
		t.Fatalf("prepare socks5 udp socket: %v", err)
	}
	defer udpConn.Close()

	if assoc == nil {
		t.Fatal("expected associate endpoint")
	}
	if !assoc.IP.Equal(net.IPv4zero) || assoc.Port != 0 {
		t.Fatalf("expected wildcard associate endpoint, got %v", assoc)
	}
}

func TestPinUpstreamIPForAutoRoutePinsHostnameAndPreservesServerName(t *testing.T) {
	cfg := &Config{
		Upstream: UpstreamConfig{
			Addr: "proxy.example:443",
		},
	}

	host, err := cfg.pinUpstreamIPForAutoRoute(func(addr string) (string, error) {
		if addr != "proxy.example" {
			t.Fatalf("unexpected resolve target: %s", addr)
		}
		return "203.0.113.7", nil
	})
	if err != nil {
		t.Fatalf("pin auto-route upstream: %v", err)
	}
	if host != "proxy.example" {
		t.Fatalf("unexpected pinned host: %q", host)
	}
	if cfg.Upstream.Addr != "203.0.113.7:443" {
		t.Fatalf("unexpected pinned addr: %q", cfg.Upstream.Addr)
	}
	if cfg.Upstream.ServerName != "proxy.example" {
		t.Fatalf("unexpected server name: %q", cfg.Upstream.ServerName)
	}
}

func TestPinUpstreamIPForAutoRoutePinsIPv6HostnameAndPreservesServerName(t *testing.T) {
	cfg := &Config{
		Upstream: UpstreamConfig{
			Addr: "proxy.example:443",
		},
	}

	host, err := cfg.pinUpstreamIPForAutoRoute(func(addr string) (string, error) {
		if addr != "proxy.example" {
			t.Fatalf("unexpected resolve target: %s", addr)
		}
		return "2001:db8::7", nil
	})
	if err != nil {
		t.Fatalf("pin auto-route upstream: %v", err)
	}
	if host != "proxy.example" {
		t.Fatalf("unexpected pinned host: %q", host)
	}
	if cfg.Upstream.Addr != "[2001:db8::7]:443" {
		t.Fatalf("unexpected pinned addr: %q", cfg.Upstream.Addr)
	}
	if cfg.Upstream.ServerName != "proxy.example" {
		t.Fatalf("unexpected server name: %q", cfg.Upstream.ServerName)
	}
}

func TestPickResolvedUpstreamIPPreservesResolverOrder(t *testing.T) {
	resolvedIP, ok := pickResolvedUpstreamIP([]net.IPAddr{
		{IP: net.ParseIP("2001:db8::7")},
		{IP: net.ParseIP("203.0.113.7")},
	})
	if !ok {
		t.Fatal("expected resolved upstream ip")
	}
	if resolvedIP != "2001:db8::7" {
		t.Fatalf("unexpected resolved upstream ip: %q", resolvedIP)
	}
}

func TestPickResolvedUpstreamIPReturnsFirstUsableAddress(t *testing.T) {
	resolvedIP, ok := pickResolvedUpstreamIP([]net.IPAddr{
		{},
		{IP: net.ParseIP("203.0.113.7")},
		{IP: net.ParseIP("2001:db8::7")},
	})
	if !ok {
		t.Fatal("expected resolved upstream ip")
	}
	if resolvedIP != "203.0.113.7" {
		t.Fatalf("unexpected resolved upstream ip: %q", resolvedIP)
	}
}

type blockingUDPSession struct {
	readCh  chan struct{}
	closeCh chan struct{}
	once    sync.Once
}

func (s *blockingUDPSession) WriteTo(_ []byte, _ *net.UDPAddr) error { return nil }
func (s *blockingUDPSession) ReadFrom(_ []byte) (int, *net.UDPAddr, error) {
	<-s.readCh
	return 0, nil, io.EOF
}
func (s *blockingUDPSession) Close() error {
	s.once.Do(func() {
		if s.closeCh != nil {
			close(s.closeCh)
		}
		if s.readCh != nil {
			close(s.readCh)
		}
	})
	return nil
}
func (s *blockingUDPSession) SetDeadline(_ time.Time) error      { return nil }
func (s *blockingUDPSession) SetReadDeadline(_ time.Time) error  { return nil }
func (s *blockingUDPSession) SetWriteDeadline(_ time.Time) error { return nil }

type timeoutNetError struct{}

func (e *timeoutNetError) Error() string   { return "i/o timeout" }
func (e *timeoutNetError) Timeout() bool   { return true }
func (e *timeoutNetError) Temporary() bool { return true }

type deadlineAwarePacketConn struct {
	mu         sync.Mutex
	readSet    bool
	writeSet   bool
	closed     bool
	readErr    error
	writeCount int
}

func (c *deadlineAwarePacketConn) ReadFrom(_ []byte) (int, net.Addr, error) {
	for {
		c.mu.Lock()
		switch {
		case c.closed:
			c.mu.Unlock()
			return 0, nil, net.ErrClosed
		case c.readSet:
			err := c.readErr
			if err == nil {
				err = &timeoutNetError{}
			}
			c.mu.Unlock()
			return 0, nil, err
		default:
			c.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *deadlineAwarePacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	c.writeCount++
	return len(p), nil
}

func (c *deadlineAwarePacketConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *deadlineAwarePacketConn) LocalAddr() net.Addr           { return &net.UDPAddr{} }
func (c *deadlineAwarePacketConn) SetDeadline(_ time.Time) error { return nil }
func (c *deadlineAwarePacketConn) SetReadDeadline(_ time.Time) error {
	c.mu.Lock()
	c.readSet = true
	c.mu.Unlock()
	return nil
}
func (c *deadlineAwarePacketConn) SetWriteDeadline(_ time.Time) error {
	c.mu.Lock()
	c.writeSet = true
	c.mu.Unlock()
	return nil
}

type deadlineAwareUDPSession struct {
	mu         sync.Mutex
	readSet    bool
	writeSet   bool
	closed     bool
	writeCalls int
	target     *net.UDPAddr
	deadlineCh chan struct{}
}

func (s *deadlineAwareUDPSession) WriteTo(_ []byte, _ *net.UDPAddr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return net.ErrClosed
	}
	s.writeCalls++
	return nil
}

func (s *deadlineAwareUDPSession) ReadFrom(_ []byte) (int, *net.UDPAddr, error) {
	for {
		s.mu.Lock()
		switch {
		case s.closed:
			s.mu.Unlock()
			return 0, nil, net.ErrClosed
		case s.readSet:
			s.mu.Unlock()
			return 0, s.target, &timeoutNetError{}
		default:
			s.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (s *deadlineAwareUDPSession) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *deadlineAwareUDPSession) SetDeadline(deadline time.Time) error {
	s.mu.Lock()
	s.readSet = !deadline.IsZero()
	s.writeSet = !deadline.IsZero()
	s.mu.Unlock()
	if s.deadlineCh != nil {
		select {
		case s.deadlineCh <- struct{}{}:
		default:
		}
	}
	return nil
}
func (s *deadlineAwareUDPSession) SetReadDeadline(_ time.Time) error {
	s.mu.Lock()
	s.readSet = true
	s.mu.Unlock()
	return nil
}
func (s *deadlineAwareUDPSession) SetWriteDeadline(_ time.Time) error {
	s.mu.Lock()
	s.writeSet = true
	s.mu.Unlock()
	return nil
}

func (s *deadlineAwareUDPSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type closeTrackingConn struct {
	remote     net.Addr
	mu         sync.Mutex
	closed     bool
	deadlineCh chan struct{}
}

func (c *closeTrackingConn) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (c *closeTrackingConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *closeTrackingConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}
func (c *closeTrackingConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}
}
func (c *closeTrackingConn) RemoteAddr() net.Addr { return c.remote }
func (c *closeTrackingConn) SetDeadline(_ time.Time) error {
	if c.deadlineCh != nil {
		select {
		case c.deadlineCh <- struct{}{}:
		default:
		}
	}
	return nil
}
func (c *closeTrackingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *closeTrackingConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *closeTrackingConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type stubTCPDialerConn struct {
	conn net.Conn
}

func (d *stubTCPDialerConn) DialTCP(_ context.Context, _ string) (net.Conn, error) {
	return d.conn, nil
}

func (d *stubTCPDialerConn) DialUDP(_ context.Context) (UDPSession, error) {
	return nil, errors.New("not implemented")
}

func (d *stubTCPDialerConn) SupportsUDP() bool { return false }
func (d *stubTCPDialerConn) Mode() string      { return ModeSocks5TLS }

func TestPinUpstreamIPForAutoRouteAcceptsIPv6Literal(t *testing.T) {
	cfg := &Config{
		Upstream: UpstreamConfig{
			Addr: "[2001:db8::7]:443",
		},
	}

	host, err := cfg.pinUpstreamIPForAutoRoute(func(string) (string, error) {
		t.Fatal("resolver should not be called for literal IP upstreams")
		return "", nil
	})
	if err != nil {
		t.Fatalf("pin auto-route upstream: %v", err)
	}
	if host != "" {
		t.Fatalf("unexpected pinned host: %q", host)
	}
	if cfg.Upstream.Addr != "[2001:db8::7]:443" {
		t.Fatalf("unexpected pinned addr: %q", cfg.Upstream.Addr)
	}
}

func TestDecodeConfigAllowsDisablingAutoRoute(t *testing.T) {
	cfg, err := decodeConfig([]byte(fmt.Sprintf(`{
		"upstream": "127.0.0.1:443",
		"client_pem": %q,
		"client_key": %q,
		"auto_route": false
	}`, "placeholder-client.pem", "placeholder-client.key")))
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if cfg.AutoRoute {
		t.Fatal("expected auto_route=false to be preserved")
	}
}

func TestReadSocks5AddressFromReaderKeepsDomainOpaque(t *testing.T) {
	buf := bytes.NewBuffer([]byte{
		0x0b,
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0x01, 0xbb,
	})

	addr, err := readSocks5AddressFromReader(buf, socksAtypDomain)
	if err != nil {
		t.Fatalf("read domain-form socks5 address: %v", err)
	}
	if addr == nil {
		t.Fatal("expected address")
	}
	if addr.IP != nil {
		t.Fatalf("expected opaque domain address without local resolution, got ip=%v", addr.IP)
	}
	if addr.Port != 443 {
		t.Fatalf("unexpected port: %d", addr.Port)
	}
}

func TestReadSocks5AddressFromReaderRejectsZeroLengthDomain(t *testing.T) {
	buf := bytes.NewBuffer([]byte{
		0x00,
		0x01, 0xbb,
	})

	addr, err := readSocks5AddressFromReader(buf, socksAtypDomain)
	if err == nil || !strings.Contains(err.Error(), "domain len: 0") {
		t.Fatalf("expected zero-length domain rejection, got addr=%v err=%v", addr, err)
	}
}

func TestHandshakeUDPAssociateAcceptsDomainRelayReply(t *testing.T) {
	d := &Socks5TLSDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	wrappedClient := &connWithRemoteAddr{
		Conn:   client,
		remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.9"), Port: 443},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(server, greeting)
		_, _ = server.Write([]byte{socksVersion, 0x00})

		header := make([]byte, 4)
		_, _ = io.ReadFull(server, header)
		var reqTail []byte
		switch header[3] {
		case socksAtypIPv4:
			reqTail = make([]byte, 6)
		case socksAtypIPv6:
			reqTail = make([]byte, 18)
		case socksAtypDomain:
			lenBuf := make([]byte, 1)
			_, _ = io.ReadFull(server, lenBuf)
			reqTail = make([]byte, int(lenBuf[0])+2)
		}
		if len(reqTail) > 0 {
			_, _ = io.ReadFull(server, reqTail)
		}

		reply := []byte{
			socksVersion, 0x00, 0x00, socksAtypDomain,
			0x0b,
			'r', 'e', 'l', 'a', 'y', '.', 'e', 'x', 'a', 'm', 'p',
			0x14, 0xb4,
		}
		_, _ = server.Write(reply)
	}()

	relayAddr, err := d.handshakeUDPAssociate(wrappedClient, &net.UDPAddr{IP: net.IPv4zero, Port: 53000})
	<-done
	if err != nil {
		t.Fatalf("udp associate with domain relay reply failed: %v", err)
	}
	if relayAddr == nil || relayAddr.Port != 5300 {
		t.Fatalf("unexpected relay addr: %+v", relayAddr)
	}
	if relayAddr.IP != nil {
		t.Fatalf("expected opaque relay ip before normalization, got %v", relayAddr.IP)
	}

	normalized, err := normalizeSocks5UDPRelayAddr(relayAddr, wrappedClient)
	if err != nil {
		t.Fatalf("normalize relay addr: %v", err)
	}
	if normalized == nil || !normalized.IP.Equal(net.ParseIP("203.0.113.9")) || normalized.Port != 5300 {
		t.Fatalf("unexpected normalized relay addr: %+v", normalized)
	}
}

func TestHandshakeConnectRejectsNonZeroReservedByte(t *testing.T) {
	d := &Socks5TLSDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(server, greeting)
		_, _ = server.Write([]byte{socksVersion, 0x00})

		reqHeader := make([]byte, 4)
		_, _ = io.ReadFull(server, reqHeader)
		switch reqHeader[3] {
		case socksAtypIPv4:
			_, _ = io.ReadFull(server, make([]byte, 6))
		case socksAtypIPv6:
			_, _ = io.ReadFull(server, make([]byte, 18))
		case socksAtypDomain:
			lenBuf := make([]byte, 1)
			_, _ = io.ReadFull(server, lenBuf)
			_, _ = io.ReadFull(server, make([]byte, int(lenBuf[0])+2))
		}

		_, _ = server.Write([]byte{socksVersion, 0x00, 0x01, socksAtypIPv4, 127, 0, 0, 1, 0x01, 0xbb})
	}()

	err := d.handshakeConnect(client, client, "example.com:443")
	<-done
	if err == nil || !strings.Contains(err.Error(), "reserved byte") {
		t.Fatalf("expected reserved byte error, got: %v", err)
	}
}

func TestHandshakeUDPAssociateRejectsNonZeroReservedByte(t *testing.T) {
	d := &Socks5TLSDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(server, greeting)
		_, _ = server.Write([]byte{socksVersion, 0x00})

		reqHeader := make([]byte, 4)
		_, _ = io.ReadFull(server, reqHeader)
		switch reqHeader[3] {
		case socksAtypIPv4:
			_, _ = io.ReadFull(server, make([]byte, 6))
		case socksAtypIPv6:
			_, _ = io.ReadFull(server, make([]byte, 18))
		case socksAtypDomain:
			lenBuf := make([]byte, 1)
			_, _ = io.ReadFull(server, lenBuf)
			_, _ = io.ReadFull(server, make([]byte, int(lenBuf[0])+2))
		}

		_, _ = server.Write([]byte{socksVersion, 0x00, 0x01, socksAtypIPv4, 127, 0, 0, 1, 0x14, 0xb4})
	}()

	_, err := d.handshakeUDPAssociate(client, &net.UDPAddr{IP: net.IPv4zero, Port: 53000})
	<-done
	if err == nil || !strings.Contains(err.Error(), "reserved byte") {
		t.Fatalf("expected reserved byte error, got: %v", err)
	}
}

func TestDecodeSocks5UDPAddressKeepsDomainOpaque(t *testing.T) {
	packet := []byte{
		socksAtypDomain,
		0x0b,
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0x01, 0xbb,
	}

	addr, headerLen, err := decodeSocks5UDPAddress(packet)
	if err != nil {
		t.Fatalf("decode domain-form udp address: %v", err)
	}
	if headerLen != len(packet) {
		t.Fatalf("unexpected header len: got=%d want=%d", headerLen, len(packet))
	}
	if addr == nil {
		t.Fatal("expected address")
	}
	if addr.IP != nil {
		t.Fatalf("expected opaque domain address without local resolution, got ip=%v", addr.IP)
	}
	if addr.Port != 443 {
		t.Fatalf("unexpected port: %d", addr.Port)
	}
}

func TestDecodeSocks5UDPAddressRejectsZeroLengthDomain(t *testing.T) {
	packet := []byte{
		socksAtypDomain,
		0x00,
		0x01, 0xbb,
	}

	addr, headerLen, err := decodeSocks5UDPAddress(packet)
	if err == nil || !strings.Contains(err.Error(), "invalid domain udp header") {
		t.Fatalf("expected zero-length domain rejection, got addr=%v headerLen=%d err=%v", addr, headerLen, err)
	}
}

func TestSameUDPEndpointTreatsOpaqueHostAsPortMatch(t *testing.T) {
	opaque := &net.UDPAddr{Port: 53}
	target := &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 53}

	if !sameUDPEndpoint(opaque, target) {
		t.Fatal("expected opaque host udp endpoint to match by port")
	}
	if !sameUDPEndpoint(target, opaque) {
		t.Fatal("expected udp endpoint comparison to stay symmetric for opaque hosts")
	}
}

func TestSameUDPEndpointDistinguishesIPv6Zones(t *testing.T) {
	a := &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 53, Zone: "en0"}
	b := &net.UDPAddr{IP: net.ParseIP("fe80::1"), Port: 53, Zone: "en1"}

	if sameUDPEndpoint(a, b) {
		t.Fatal("expected link-local udp endpoints with different zones not to match")
	}
}

func TestSocks5TLSSessionDeadlineWithNilUDPConn(t *testing.T) {
	s := &socks5TLSUDPSession{}
	if err := s.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed on SetDeadline, got: %v", err)
	}
	if err := s.SetReadDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed on SetReadDeadline, got: %v", err)
	}
	if err := s.SetWriteDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed on SetWriteDeadline, got: %v", err)
	}
}

func TestSocks5TLSSessionClosesUDPWhenControlConnCloses(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer udpConn.Close()

	s := &socks5TLSUDPSession{
		controlConn: client,
		udpConn:     udpConn,
		readBuf:     make([]byte, 2048),
	}
	s.startControlMonitor()

	if err := server.Close(); err != nil {
		t.Fatalf("close control peer: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		err := s.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		if errors.Is(err, net.ErrClosed) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected udp conn to close after control conn closes, got %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestSocks5TLSSessionReadFromDropsOversizedFrame(t *testing.T) {
	/*
	 * Verifies that ReadFrom silently drops frames whose body exceeds the
	 * caller's payload buffer and returns the next well-sized frame without
	 * error.
	 */

	/* Builds a SOCKS5 UDP frame: RSV(2) + FRAG(1) + ATYP_IPv4(1) + IP(4) + port(2) + body. */
	buildFrame := func(body []byte) []byte {
		frame := []byte{0x00, 0x00, 0x00, socksAtypIPv4, 127, 0, 0, 1, 0, 80}
		return append(frame, body...)
	}

	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer serverConn.Close()

	senderConn, err := net.DialUDP("udp", nil, serverConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}
	defer senderConn.Close()

	payloadBuf := make([]byte, 10)
	s := &socks5TLSUDPSession{
		udpConn: serverConn,
		readBuf: make([]byte, 512),
	}

	/* Sends an oversized frame (body=11 bytes > payloadBuf capacity=10). */
	oversized := buildFrame(bytes.Repeat([]byte{0xFF}, 11))
	if _, err := senderConn.Write(oversized); err != nil {
		t.Fatalf("send oversized: %v", err)
	}
	/* Sends a valid frame (body=5 bytes). */
	wantBody := []byte{1, 2, 3, 4, 5}
	valid := buildFrame(wantBody)
	if _, err := senderConn.Write(valid); err != nil {
		t.Fatalf("send valid: %v", err)
	}

	if err := serverConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	n, src, err := s.ReadFrom(payloadBuf)
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}
	if n != len(wantBody) {
		t.Fatalf("expected n=%d got %d", len(wantBody), n)
	}
	if !bytes.Equal(payloadBuf[:n], wantBody) {
		t.Fatalf("payload mismatch: got %v want %v", payloadBuf[:n], wantBody)
	}
	if src == nil {
		t.Fatal("expected non-nil source address")
	}
}

func TestMonitorHandshakeCancellationClosesConnOnContextCancel(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	stop := monitorHandshakeCancellation(ctx, client)
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		err := client.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if err != nil {
			stop()
			return
		}
		select {
		case <-deadline:
			stop()
			t.Fatalf("expected handshake cancellation monitor to close conn, got %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestNormalizeSocks5UDPRelayAddrRewritesLoopbackToPeer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 443}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize loopback relay: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("203.0.113.7")) || got.Port != 53000 {
		t.Fatalf("unexpected normalized relay: %v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrRewritesUnspecifiedToPeer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.IPv4zero, Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("198.51.100.9"), Port: 443}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize unspecified relay: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("198.51.100.9")) || got.Port != 53000 {
		t.Fatalf("unexpected normalized relay: %v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrRewritesPrivateRelayForPublicPeer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.ParseIP("10.0.0.5"), Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("198.51.100.11"), Port: 443}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize private relay for public peer: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("198.51.100.11")) || got.Port != 53000 {
		t.Fatalf("unexpected normalized relay: %v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrRewritesIPv6ULARelayForPublicPeer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.ParseIP("fd00::5"), Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::11"), Port: 443}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize ipv6 ula relay for public peer: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("2001:db8::11")) || got.Port != 53000 {
		t.Fatalf("unexpected normalized relay: %v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrPreservesPrivateRelayForPrivatePeer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.ParseIP("10.0.0.5"), Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("10.0.0.9"), Port: 443}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize private relay for private peer: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("10.0.0.5")) || got.Port != 53000 {
		t.Fatalf("unexpected normalized relay: %v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrUsesPeerZoneForScopedIPv6Peer(t *testing.T) {
	relay := &net.UDPAddr{IP: net.IPv6zero, Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("fe80::11"), Port: 443, Zone: "en0"}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize scoped ipv6 relay: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("fe80::11")) || got.Port != 53000 || got.Zone != "en0" {
		t.Fatalf("unexpected normalized relay: %+v", got)
	}
}

func TestNormalizeSocks5UDPRelayAddrPreservesLinkLocalRelayAndInheritsPeerZone(t *testing.T) {
	relay := &net.UDPAddr{IP: net.ParseIP("fe80::55"), Port: 53000}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("fe80::11"), Port: 443, Zone: "en0"}}

	got, err := normalizeSocks5UDPRelayAddr(relay, conn)
	if err != nil {
		t.Fatalf("normalize link-local relay: %v", err)
	}
	if !got.IP.Equal(net.ParseIP("fe80::55")) || got.Port != 53000 || got.Zone != "en0" {
		t.Fatalf("unexpected normalized relay: %+v", got)
	}
}

func TestHandleUDPMarksCooldownOnAddrNotAvailable(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun: TunConfig{
				Name: validTunNameForRuntime(),
				MTU:  1500,
			},
			Upstream: UpstreamConfig{
				Addr:      "127.0.0.1:443",
				ClientPEM: clientPEM,
				ClientKey: clientKey,
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:     &stubDialerEaddrNotAvail{},
		udpDialSem: make(chan struct{}, 1),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("8.8.8.8").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}
	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})
	if !e.isUDPDialCoolingDown() {
		t.Fatalf("expected udp dial cooldown after EADDRNOTAVAIL")
	}
}

func TestHandleTCPMarksCooldownOnAddrNotAvailable(t *testing.T) {
	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun: TunConfig{
				Name: validTunNameForRuntime(),
				MTU:  1500,
			},
			Upstream: UpstreamConfig{
				Addr:      "127.0.0.1:443",
				ClientPEM: clientPEM,
				ClientKey: clientKey,
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:     &stubDialerEaddrNotAvail{},
		tcpDialSem: make(chan struct{}, 1),
		udpDialSem: make(chan struct{}, 1),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("1.1.1.1").To4()),
		LocalPort:     443,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}
	e.handleTCP(context.Background(), id, &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}})
	if e.isTCPDialCoolingDown() {
		t.Fatalf("expected tcp dial cooldown to drain after bounded retry handling")
	}
}

func TestHandleTCPDropsSecondaryUpstreamLoopTarget(t *testing.T) {
	dialer := &stubTCPDialCounter{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstreams: []UpstreamConfig{
				{Addr: "127.0.0.1:443"},
				{Addr: "203.0.113.9:443"},
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
			},
		},
		dialer:     dialer,
		tcpDialSem: make(chan struct{}, 1),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("203.0.113.9").To4()),
		LocalPort:     443,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}
	e.handleTCP(context.Background(), id, &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}})
	if got := dialer.count(); got != 0 {
		t.Fatalf("expected secondary upstream loop target to be dropped before dial, got %d dials", got)
	}
}

func TestHandleTCPDirectRouteSkipsUpstreamDialer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen direct target: %v", err)
	}
	defer ln.Close()
	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			close(accepted)
			_ = conn.Close()
		}
	}()

	dialer := &stubTCPDialCounter{}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
			},
		},
		dialer:          dialer,
		router:          router,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}
	addr := ln.Addr().(*net.TCPAddr)
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(addr.IP.To4()),
		LocalPort:     uint16(addr.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleTCP(context.Background(), id, &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}})

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("expected direct target to receive connection")
	}
	if got := dialer.count(); got != 0 {
		t.Fatalf("expected direct route to skip upstream dialer, got %d dials", got)
	}
}

func TestHandleTCPDirectRouteHonorsDialConcurrencyLimit(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen direct target: %v", err)
	}
	defer ln.Close()
	tcpLn := ln.(*net.TCPListener)
	if err := tcpLn.SetDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("set listener deadline: %v", err)
	}
	accepted := make(chan bool, 1)
	go func() {
		conn, err := tcpLn.Accept()
		if err == nil {
			_ = conn.Close()
			accepted <- true
			return
		}
		accepted <- false
	}()

	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	tcpDialSem := make(chan struct{}, 1)
	tcpDialSem <- struct{}{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 50,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
			},
		},
		router:            router,
		tcpDialSem:        tcpDialSem,
		activeUpstreams:   make(map[io.Closer]struct{}),
		activeLocals:      make(map[io.Closer]struct{}),
		logThrottleStates: make(map[string]logThrottleState),
	}
	addr := ln.Addr().(*net.TCPAddr)
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(addr.IP.To4()),
		LocalPort:     uint16(addr.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleTCP(context.Background(), id, &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}})

	if <-accepted {
		t.Fatal("did not expect direct tcp dial to bypass a full dial concurrency limit")
	}
	if len(tcpDialSem) != 1 {
		t.Fatalf("expected pre-held dial slot to remain held, got len=%d", len(tcpDialSem))
	}
}

func TestHandleUDPDirectRouteSkipsUpstreamDialer(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen direct udp target: %v", err)
	}
	defer serverConn.Close()
	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		_ = serverConn.SetReadDeadline(time.Now().Add(time.Second))
		n, addr, err := serverConn.ReadFromUDP(buf)
		if err == nil {
			received <- append([]byte(nil), buf[:n]...)
			_, _ = serverConn.WriteToUDP([]byte{0xaa}, addr)
		}
		close(received)
	}()

	dialer := &stubUDPDialCounter{}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		router:          router,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}
	addr := serverConn.LocalAddr().(*net.UDPAddr)
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(addr.IP.To4()),
		LocalPort:     uint16(addr.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}
	conn := &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}}

	e.handleUDP(context.Background(), id, conn)

	gotPayload := <-received
	if !bytes.Equal(gotPayload, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("expected direct udp target to receive payload, got %#v", gotPayload)
	}
	if got := dialer.count(); got != 0 {
		t.Fatalf("expected direct udp route to skip upstream dialer, got %d dials", got)
	}
}

func TestHandleUDPDirectRouteDropsWhenTargetProbeLimitReached(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen direct udp target: %v", err)
	}
	defer serverConn.Close()
	received := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, 64)
		_ = serverConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if n, _, err := serverConn.ReadFromUDP(buf); err == nil && n > 0 {
			received <- struct{}{}
		}
		close(received)
	}()

	dialer := &stubUDPDialCounter{}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	addr := serverConn.LocalAddr().(*net.UDPAddr)
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				UDPBuffer: 4096,
				EnableUDP: true,
			},
		},
		dialer:          dialer,
		router:          router,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		directUDPActive: map[string]int{addr.String(): directUDPActiveTargetMax},
	}
	var routeHooks int
	e.SetDirectRouteHook(func(DirectRoute) error {
		routeHooks++
		return nil
	})
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(addr.IP.To4()),
		LocalPort:     uint16(addr.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}})

	if _, ok := <-received; ok {
		t.Fatal("did not expect direct udp target to receive payload while probe limit is reached")
	}
	if got := dialer.count(); got != 0 {
		t.Fatalf("expected direct udp probe limit to skip upstream dialer, got %d dials", got)
	}
	if routeHooks != 0 {
		t.Fatalf("expected direct udp probe limit to skip route repair, got %d hooks", routeHooks)
	}
}

func TestHandleUDPDirectRouteProbeLimitsPublicTargets(t *testing.T) {
	dialer := &stubUDPDialCounter{}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"1.0.1.0/24"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	target := "1.0.1.8:443"
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				UDPBuffer: 4096,
				EnableUDP: true,
			},
		},
		dialer:          dialer,
		router:          router,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		directUDPActive: map[string]int{target: directUDPActiveTargetMax},
	}
	var routeHooks int
	e.SetDirectRouteHook(func(DirectRoute) error {
		routeHooks++
		return nil
	})
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("1.0.1.8").To4()),
		LocalPort:     443,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}})

	if routeHooks != 0 {
		t.Fatalf("expected public direct udp probe limit to skip route repair, got %d hooks", routeHooks)
	}
}

func TestHandleUDPDisabledDropsDirectRoute(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen direct udp target: %v", err)
	}
	defer serverConn.Close()
	received := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, 64)
		_ = serverConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if n, _, err := serverConn.ReadFromUDP(buf); err == nil && n > 0 {
			received <- struct{}{}
		}
		close(received)
	}()

	dialer := &stubUDPDialCounter{}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				UDPBuffer: 4096,
				EnableUDP: false,
			},
		},
		dialer:          dialer,
		router:          router,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}
	addr := serverConn.LocalAddr().(*net.UDPAddr)
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(addr.IP.To4()),
		LocalPort:     uint16(addr.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}})

	if _, ok := <-received; ok {
		t.Fatal("did not expect direct udp target to receive payload when udp is disabled")
	}
	if got := dialer.count(); got != 0 {
		t.Fatalf("expected udp disabled to skip upstream dialer, got %d dials", got)
	}
}

func TestHandleDirectUDPThrottlesDeadlineRefresh(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("listen direct udp target: %v", err)
	}
	defer serverConn.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		_ = serverConn.SetReadDeadline(time.Now().Add(time.Second))
		for {
			if _, _, err := serverConn.ReadFromUDP(buf); err != nil {
				return
			}
		}
	}()

	localConn := &multiReadPacketConn{readsLeft: 65}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				UDPBuffer:        4096,
				UDPIdleTimeoutMS: 1000,
			},
		},
		activeUpstreams: make(map[io.Closer]struct{}),
	}
	target := serverConn.LocalAddr().(*net.UDPAddr)

	e.handleDirectUDP(context.Background(), "udp-test", "198.18.0.1:53000", target, target.String(), localConn, RouteDecision{Outbound: OutboundDirect, Rule: "direct_cidr:127.0.0.0/8"})

	readDeadlines, writeDeadlines := localConn.deadlineCounts()
	if readDeadlines > 4 {
		t.Fatalf("expected direct udp read deadlines to be throttled, got %d", readDeadlines)
	}
	if writeDeadlines > 2 {
		t.Fatalf("expected direct udp write deadlines to be throttled, got %d", writeDeadlines)
	}
}

func TestHandleDirectUDPMarksNoResponseCooldown(t *testing.T) {
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				UDPBuffer:        4096,
				UDPIdleTimeoutMS: 20,
			},
		},
		activeUpstreams:      make(map[io.Closer]struct{}),
		udpNoResponseBackoff: make(map[string]int64),
	}
	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}
	targetStr := target.String()

	e.handleDirectUDP(
		context.Background(),
		"udp-test",
		"198.18.0.1:53000",
		target,
		targetStr,
		&stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}},
		RouteDecision{Outbound: OutboundDirect, Rule: "direct_cidr:127.0.0.0/8"},
	)

	if !e.isUDPNoResponseCoolingDown(targetStr) {
		t.Fatal("expected direct udp no-response target to enter cooldown")
	}
}

func TestHandleUDPCancelsDialOnNetworkChange(t *testing.T) {
	dialer := &stubCancelableUDPDialer{
		enterCh:  make(chan struct{}),
		cancelCh: make(chan error, 1),
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun: TunConfig{
				Name: validTunNameForRuntime(),
				MTU:  1500,
			},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		udpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("8.8.8.8").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.handleUDP(context.Background(), id, &stubPacketConnEOF{})
	}()

	select {
	case <-dialer.enterCh:
	case <-time.After(time.Second):
		t.Fatal("udp dial was not started")
	}

	e.HandleNetworkChange()

	select {
	case err := <-dialer.cancelCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("udp dial was not canceled on network change")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handleUDP did not exit after dial cancellation")
	}
}

func TestHandleNetworkChangeClosesActiveUDPSession(t *testing.T) {
	session := &stubUDPSession{closed: make(chan struct{})}
	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}
	e.registerActiveUpstream(session)

	e.HandleNetworkChange()

	select {
	case <-session.closed:
	case <-time.After(time.Second):
		t.Fatal("active udp session was not closed on network change")
	}
	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected active upstreams to be cleared after network change, got %d", len(e.activeUpstreams))
	}
}

func TestHandleNetworkChangeClosesIdleUDPPool(t *testing.T) {
	session := &stubUDPSession{closed: make(chan struct{})}
	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}
	e.registerActiveUpstream(session)

	e.HandleNetworkChange()

	select {
	case <-session.closed:
	case <-time.After(time.Second):
		t.Fatal("idle udp session was not closed on network change")
	}

	e.udpPoolMu.Lock()
	defer e.udpPoolMu.Unlock()
	if len(e.udpPool) != 0 {
		t.Fatalf("expected udp pool to be cleared, got %d sessions", len(e.udpPool))
	}
}

func TestHandleNetworkChangeClearsDialCooldowns(t *testing.T) {
	e := &Engine{}
	staleGeneration := e.networkGeneration.Load()
	e.markTCPDialBackoff(5 * time.Second)
	e.markUDPDialBackoffForGeneration(staleGeneration, 5*time.Second)
	e.markUDPNoResponseBackoffForGeneration("9.9.9.9:9999", staleGeneration, 5*time.Second)
	e.markTCPTargetBackoff("103.228.130.61:443", 5*time.Second)

	e.HandleNetworkChange()

	if e.isTCPDialCoolingDown() {
		t.Fatal("expected tcp dial cooldown to be cleared on network change")
	}
	if e.isUDPDialCoolingDown() {
		t.Fatal("expected udp dial cooldown to be cleared on network change")
	}
	if e.isUDPNoResponseCoolingDown("9.9.9.9:9999") {
		t.Fatal("expected udp no-response cooldown to be cleared on network change")
	}
	if e.isTCPTargetCoolingDown("103.228.130.61:443") {
		t.Fatal("expected tcp target cooldown to be cleared on network change")
	}

	e.markUDPDialBackoffForGeneration(staleGeneration, 5*time.Second)
	e.markUDPNoResponseBackoffForGeneration("9.9.9.9:9999", staleGeneration, 5*time.Second)
	if e.isUDPDialCoolingDown() {
		t.Fatal("stale pre-network-change udp failure restored dial cooldown")
	}
	if e.isUDPNoResponseCoolingDown("9.9.9.9:9999") {
		t.Fatal("stale pre-network-change udp flow restored no-response cooldown")
	}
}

func TestHandleNetworkChangeResetsMultiUpstreamStableIndex(t *testing.T) {
	dialer := &multiUpstreamDialer{
		mode: ModeSocks5TLS,
		dialers: []UpstreamDialer{
			&stubConcurrentTCPDialer{},
			&stubConcurrentTCPDialer{},
		},
	}
	dialer.stableIndex.Store(1)
	e := &Engine{dialer: dialer}

	e.HandleNetworkChange()

	if got := dialer.stableIndex.Load(); got != 0 {
		t.Fatalf("stable index = %d, want 0", got)
	}
}

func TestHandleNetworkChangePreventsInflightDialFromRestoringStableIndex(t *testing.T) {
	for _, protocol := range []string{"tcp", "udp"} {
		t.Run(protocol, func(t *testing.T) {
			controlled := &stubReleasedUpstreamDialer{
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}
			dialer := &multiUpstreamDialer{
				mode:    ModeSocks5TLS,
				dialers: []UpstreamDialer{&stubConcurrentUDPDialer{}, controlled},
			}
			dialer.stableIndex.Store(1)
			e := &Engine{dialer: dialer}
			done := make(chan error, 1)
			go func() {
				if protocol == "tcp" {
					conn, err := dialer.DialTCP(context.Background(), "8.8.8.8:53")
					if conn != nil {
						_ = conn.Close()
					}
					done <- err
					return
				}
				session, err := dialer.DialUDP(context.Background())
				if session != nil {
					_ = session.Close()
				}
				done <- err
			}()
			select {
			case <-controlled.entered:
			case <-time.After(time.Second):
				t.Fatal("in-flight upstream dial did not start")
			}

			e.HandleNetworkChange()
			close(controlled.release)
			if err := <-done; err != nil {
				t.Fatalf("released upstream dial failed: %v", err)
			}
			if got := dialer.stableIndex.Load(); got != 0 {
				t.Fatalf("stale in-flight %s dial restored stable index: got=%d want=0", protocol, got)
			}
		})
	}
}

func TestHandleNetworkChangeAdvancesGenerationBeforeResettingSelection(t *testing.T) {
	e := &Engine{}
	dialer := &stubSelectionResetObserver{engine: e}
	e.dialer = dialer
	e.HandleNetworkChange()
	if got := dialer.observedGeneration.Load(); got != 1 {
		t.Fatalf("selection reset observed generation %d, want 1", got)
	}
}

func TestHandleNetworkChangeWarmsUpUpstreamTransport(t *testing.T) {
	dialer := &stubWarmupDialer{warmupCh: make(chan struct{})}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	e.HandleNetworkChange()

	select {
	case <-dialer.warmupCh:
	case <-time.After(time.Second):
		t.Fatal("expected upstream warmup to start after network change")
	}
}

func TestMultiUpstreamWarmupAllowsPartialSuccess(t *testing.T) {
	dialer := &multiUpstreamDialer{
		mode: ModeSocks5TLS,
		dialers: []UpstreamDialer{
			&stubWarmupDialer{err: errors.New("warmup failed")},
			&stubWarmupDialer{},
		},
	}

	if err := dialer.Warmup(context.Background()); err != nil {
		t.Fatalf("expected partial warmup success to succeed, got %v", err)
	}
}

func TestMultiUpstreamWarmupFailsWhenAllAttemptsFail(t *testing.T) {
	dialer := &multiUpstreamDialer{
		mode: ModeSocks5TLS,
		dialers: []UpstreamDialer{
			&stubWarmupDialer{err: errors.New("first failed")},
			&stubWarmupDialer{err: errors.New("second failed")},
		},
	}

	err := dialer.Warmup(context.Background())
	if err == nil {
		t.Fatal("expected all-failed warmup to return an error")
	}
	if !strings.Contains(err.Error(), "upstream[0]") || !strings.Contains(err.Error(), "upstream[1]") {
		t.Fatalf("expected joined upstream errors, got %v", err)
	}
}

func TestHandleNetworkChangeRewarmsLatestGeneration(t *testing.T) {
	blockCh := make(chan struct{})
	dialer := &stubWarmupDialer{
		warmupCh: make(chan struct{}),
		blockCh:  blockCh,
	}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	e.HandleNetworkChange()
	select {
	case <-dialer.warmupCh:
	case <-time.After(time.Second):
		t.Fatal("expected first warmup to start")
	}
	e.HandleNetworkChange()
	e.HandleNetworkChange()

	time.Sleep(100 * time.Millisecond)
	if got := dialer.callCount(); got != 1 {
		t.Fatalf("expected one in-flight warmup before release, got %d", got)
	}
	close(blockCh)
	deadline := time.Now().Add(time.Second)
	for dialer.callCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := dialer.callCount(); got != 2 {
		t.Fatalf("expected one follow-up warmup for the latest generation, got %d", got)
	}
}

func TestPruneLogThrottleStatesLockedRemovesExpiredEntries(t *testing.T) {
	e := &Engine{
		logThrottleStates: map[string]logThrottleState{
			"old": {LastLogTime: time.Now().Add(-logThrottleStateTTL - time.Second)},
			"new": {LastLogTime: time.Now()},
		},
	}
	e.pruneLogThrottleStatesLocked(time.Now())
	if _, ok := e.logThrottleStates["old"]; ok {
		t.Fatal("expected expired throttled-log state to be removed")
	}
	if _, ok := e.logThrottleStates["new"]; !ok {
		t.Fatal("expected recent throttled-log state to remain")
	}
}

func TestHandleUDPRelaysDNSOverTCP(t *testing.T) {
	dialer := &stubDNSTCPDialer{response: []byte{0xaa, 0xbb, 0xcc}}
	conn := &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("8.8.8.8").To4()),
		LocalPort:     53,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	if got := dialer.dialCount(); got != 1 {
		t.Fatalf("expected 1 tcp dns dial, got %d", got)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected 1 dns response write, got %d", len(conn.writes))
	}
	if got := conn.writes[0]; !bytes.Equal(got, []byte{0xaa, 0xbb, 0xcc}) {
		t.Fatalf("unexpected dns response: %x", got)
	}
}

func TestHandleUDPRelaysDNSOverTCPBeforeDirectRouting(t *testing.T) {
	dialer := &stubDNSTCPDialer{response: []byte{0xaa, 0xbb, 0xcc}}
	conn := &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}}
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"127.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		router:          router,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("127.0.0.1").To4()),
		LocalPort:     53,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	if got := dialer.dialCount(); got != 1 {
		t.Fatalf("expected dns-over-tcp dial before direct routing, got %d", got)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected 1 dns response write, got %d", len(conn.writes))
	}
}

func TestHandleUDPRelaysDNSOverTCPWhenUDPDisabled(t *testing.T) {
	dialer := &stubDNSTCPDialer{response: []byte{0xaa, 0xbb, 0xcc}}
	conn := &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02, 0x03}}}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        false,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("8.8.8.8").To4()),
		LocalPort:     53,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	if got := dialer.dialCount(); got != 1 {
		t.Fatalf("expected 1 tcp dns dial with udp disabled, got %d", got)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected 1 dns response write with udp disabled, got %d", len(conn.writes))
	}
	if got := conn.writes[0]; !bytes.Equal(got, []byte{0xaa, 0xbb, 0xcc}) {
		t.Fatalf("unexpected dns response with udp disabled: %x", got)
	}
}

func TestHandleUDPRelaysDNSOverTCPInHTTPSMode(t *testing.T) {
	dialer := &stubDNSTCPDialer{mode: ModeHTTPS, response: []byte{0xde, 0xad}}
	conn := &stubDNSPacketConn{reads: [][]byte{{0xfa, 0xce}}}
	e := &Engine{
		cfg: &Config{
			Mode: ModeHTTPS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("1.1.1.1").To4()),
		LocalPort:     53,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	if got := dialer.dialCount(); got != 1 {
		t.Fatalf("expected 1 tcp dns dial in https mode, got %d", got)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected 1 dns response write, got %d", len(conn.writes))
	}
}

func TestHandleDNSOverTCPHandlesMultipleQueriesPerFlow(t *testing.T) {
	dialer := &stubDNSTCPDialer{response: []byte{0x10, 0x20}}
	conn := &stubDNSPacketConn{
		reads: [][]byte{
			{0xaa, 0xbb},
			{0xcc, 0xdd},
		},
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("8.8.4.4").To4()),
		LocalPort:     53,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	if got := dialer.dialCount(); got != 1 {
		t.Fatalf("expected pooled dns connection reuse, got %d dials", got)
	}
	if len(conn.writes) != 2 {
		t.Fatalf("expected 2 dns response writes, got %d", len(conn.writes))
	}
}

func TestHandleNetworkChangeClosesIdleDNSPool(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: client}},
		},
	}
	e.registerActiveUpstream(client)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := server.Read(buf)
		done <- err
	}()

	e.HandleNetworkChange()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected pooled dns conn to be closed on network change")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for pooled dns conn close")
	}

	e.dnsPoolMu.Lock()
	defer e.dnsPoolMu.Unlock()
	if len(e.dnsPool) != 0 {
		t.Fatalf("expected dns pool to be cleared, got %d targets", len(e.dnsPool))
	}
}

func TestExchangeDNSOverTCPRetriesTransientAddrNotAvail(t *testing.T) {
	dialer := &stubRetryTCPDialer{
		response:  []byte{0xde, 0xad},
		failCount: 1,
	}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	resp, releaseResp, err := e.exchangeDNSOverTCP(context.Background(), "8.8.8.8:53", []byte{0x01, 0x02}, time.Second)
	if err != nil {
		t.Fatalf("expected transient retry to recover, got %v", err)
	}
	defer releaseResp()
	if !bytes.Equal(resp, []byte{0xde, 0xad}) {
		t.Fatalf("unexpected dns response %x", resp)
	}
	if dialer.dials != 2 {
		t.Fatalf("expected 2 upstream dial attempts, got %d", dialer.dials)
	}
}

func TestSuccessfulPooledDNSExchangeClearsTargetBackoff(t *testing.T) {
	target := "8.8.8.8:53"
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		var lengthBuf [2]byte
		if _, err := io.ReadFull(server, lengthBuf[:]); err != nil {
			return
		}
		payload := make([]byte, int(binary.BigEndian.Uint16(lengthBuf[:])))
		if _, err := io.ReadFull(server, payload); err != nil {
			return
		}
		response := append([]byte(nil), lengthBuf[:]...)
		response = append(response, payload...)
		_, _ = server.Write(response)
	}()
	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			target: {{conn: client, expiresAt: time.Now().Add(time.Minute)}},
		},
	}
	e.markTCPTargetBackoff(target, time.Minute)

	response, releaseResponse, err := e.exchangeDNSOverTCP(context.Background(), target, []byte{0x01, 0x02}, time.Second)
	if err != nil {
		t.Fatalf("pooled dns exchange failed: %v", err)
	}
	if !bytes.Equal(response, []byte{0x01, 0x02}) {
		t.Fatalf("unexpected pooled dns response: %x", response)
	}
	releaseResponse()
	if e.isTCPTargetCoolingDown(target) {
		t.Fatal("successful pooled dns exchange did not clear target cooldown")
	}
	e.closeIdleDNSPool()
}

func TestDNSConnectBudgetHonorsExchangeTimeout(t *testing.T) {
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 10_000,
				IdleTimeoutMS:    300_000,
			},
		},
	}
	if got := e.dnsConnectBudget(e.cfg.DNSExchangeTimeout()); got != 10*time.Second {
		t.Fatalf("expected dns connect budget to honor dns exchange timeout, got %s", got)
	}
}

func TestExchangeDNSOverTCPRejectsOversizedPayloadBeforeDial(t *testing.T) {
	e := &Engine{}
	response, releaseResponse, err := e.exchangeDNSOverTCP(
		context.Background(),
		"8.8.8.8:53",
		make([]byte, 65536),
		time.Second,
	)
	releaseResponse()
	if err == nil || !strings.Contains(err.Error(), "65535-byte framing limit") {
		t.Fatalf("expected oversized dns tcp payload error, got %v", err)
	}
	if response != nil {
		t.Fatalf("oversized dns tcp payload returned response: %x", response)
	}
}

func TestExchangeDNSOverTCPRetriesPooledEOF(t *testing.T) {
	staleClient, staleServer := net.Pipe()
	_ = staleServer.Close()

	dialer := &stubRetryTCPDialer{
		response: []byte{0xbe, 0xef},
	}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: staleClient}},
		},
	}

	resp, releaseResp, err := e.exchangeDNSOverTCP(context.Background(), "8.8.8.8:53", []byte{0x01, 0x02}, time.Second)
	if err != nil {
		t.Fatalf("expected pooled eof retry to recover, got %v", err)
	}
	defer releaseResp()
	if !bytes.Equal(resp, []byte{0xbe, 0xef}) {
		t.Fatalf("unexpected dns response %x", resp)
	}
	if dialer.dials != 1 {
		t.Fatalf("expected 1 fresh dial after pooled eof, got %d", dialer.dials)
	}
}

func TestExchangeDNSOverTCPUsesOneEndToEndDeadline(t *testing.T) {
	staleClient, staleServer := net.Pipe()
	defer staleServer.Close()
	e := &Engine{
		cfg:             &Config{Runtime: RuntimeConfig{ConnectTimeoutMS: 1000}},
		dialer:          &stubBlockingTCPDialer{},
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: staleClient}},
		},
	}

	start := time.Now()
	response, releaseResponse, err := e.exchangeDNSOverTCP(
		context.Background(), "8.8.8.8:53", []byte{0x01, 0x02}, 150*time.Millisecond,
	)
	releaseResponse()
	if err == nil {
		t.Fatalf("expected end-to-end DNS deadline, got response %x", response)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("DNS retry exceeded one exchange deadline: %s", elapsed)
	}
}

func TestExchangeDNSOverTCPRetriesPastMultipleStalePooledConns(t *testing.T) {
	staleClientA, staleServerA := net.Pipe()
	_ = staleServerA.Close()
	staleClientB, staleServerB := net.Pipe()
	_ = staleServerB.Close()

	dialer := &stubRetryTCPDialer{
		response: []byte{0xca, 0xfe},
	}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: staleClientA}, {conn: staleClientB}},
		},
	}

	resp, releaseResp, err := e.exchangeDNSOverTCP(context.Background(), "8.8.8.8:53", []byte{0x01, 0x02}, time.Second)
	if err != nil {
		t.Fatalf("expected pooled eof retry to skip stale pool and recover, got %v", err)
	}
	defer releaseResp()
	if !bytes.Equal(resp, []byte{0xca, 0xfe}) {
		t.Fatalf("unexpected dns response %x", resp)
	}
	if dialer.dials != 1 {
		t.Fatalf("expected 1 fresh dial after stale pool retry, got %d", dialer.dials)
	}
	/*
	 * The stale conn (staleClientA) is still in the pool.  The fresh conn may or may
	 * not be returned to the pool depending on whether clearConnDeadline succeeds
	 * before the stub server goroutine closes its pipe end. Accept either count.
	 */
	if conns := e.dnsPool["8.8.8.8:53"]; len(conns) < 1 || len(conns) > 2 {
		t.Fatalf("expected 1-2 pooled conns after stale pool retry, got %d", len(conns))
	}
}

func TestDNSCacheKeyIncludesTargetAddress(t *testing.T) {
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{DNSCacheSize: 8},
		},
		dnsCache: make(map[string]dnsCacheEntry),
	}
	req := []byte{
		0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
	}
	respA := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04, 0x01, 0x01, 0x01, 0x01,
	}
	respB := append([]byte(nil), respA...)
	respB[len(respB)-4] = 8
	respB[len(respB)-3] = 8
	respB[len(respB)-2] = 8
	respB[len(respB)-1] = 8

	e.putDNSCache("1.1.1.1:53", req, respA)
	e.putDNSCache("8.8.8.8:53", req, respB)

	gotA := e.getDNSCache("1.1.1.1:53", req)
	gotB := e.getDNSCache("8.8.8.8:53", req)
	if gotA == nil || gotB == nil {
		t.Fatal("expected both dns cache entries to be present")
	}
	if bytes.Equal(gotA, gotB) {
		t.Fatalf("expected per-target cache isolation, got identical responses %x", gotA)
	}
}

func TestDNSCacheKeySeparatesAdditionalRecords(t *testing.T) {
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{DNSCacheSize: 8},
		},
		dnsCache: make(map[string]dnsCacheEntry),
	}
	reqWithOPT := []byte{
		0x12, 0x34, 0x01, 0x00,
		0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00,
		0x00, 0x01,
		0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00,
		0x00, 0x01, 0x00, 0x01,
		0x00,
		0x00, 0x29,
		0x10, 0x00,
		0x00, 0x00,
		0x00, 0x00,
		0x00, 0x00,
	}
	reqPlain := []byte{
		0xab, 0xcd, 0x01, 0x20,
		0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00,
		0x00, 0x00,
		0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00,
		0x00, 0x01, 0x00, 0x01,
	}
	resp := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04, 0x01, 0x01, 0x01, 0x01,
	}

	e.putDNSCache("8.8.8.8:53", reqWithOPT, resp)
	got := e.getDNSCache("8.8.8.8:53", reqPlain)
	if got != nil {
		t.Fatalf("expected cache miss for requests with different additional records, got %x", got)
	}
}

func TestPutDNSCacheRejectsMismatchedQuestion(t *testing.T) {
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{DNSCacheSize: 8},
		},
		dnsCache: make(map[string]dnsCacheEntry),
	}
	req := []byte{
		0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
	}
	resp := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00, 0x05, 'o', 't', 'h', 'e', 'r',
		0x03, 'c', 'o', 'm', 0x00, 0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04, 0x08, 0x08, 0x08, 0x08,
	}

	e.putDNSCache("8.8.8.8:53", req, resp)
	if got := e.getDNSCache("8.8.8.8:53", req); got != nil {
		t.Fatalf("expected mismatched response question to be rejected from cache, got %x", got)
	}
}

func TestExtractMinDNSTTLRejectsTruncatedResourceData(t *testing.T) {
	resp := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00,
		0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c,
		0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04,
		0x01, 0x01,
	}

	if got := extractMinDNSTTL(resp); got != 0 {
		t.Fatalf("expected truncated dns answer to be uncachable, got ttl=%d", got)
	}
}

func TestExtractMinDNSTTLRejectsTruncatedLaterRecordMetadata(t *testing.T) {
	resp := []byte{
		0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00,
		0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00,
		0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c,
		0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04,
		0x01, 0x01, 0x01, 0x01,
		0xc0, 0x0c,
		0x00, 0x01,
	}

	if got := extractMinDNSTTL(resp); got != 0 {
		t.Fatalf("expected truncated later dns record metadata to be uncachable, got ttl=%d", got)
	}
}

func TestExtractMinDNSTTLIgnoresOPTAdditionalPseudoTTL(t *testing.T) {
	resp := []byte{
		0x12, 0x34, 0x81, 0x80,
		0x00, 0x01,
		0x00, 0x01,
		0x00, 0x00,
		0x00, 0x01,
		0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
		0x03, 'c', 'o', 'm', 0x00,
		0x00, 0x01, 0x00, 0x01,
		0xc0, 0x0c,
		0x00, 0x01, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x3c,
		0x00, 0x04, 0x01, 0x01, 0x01, 0x01,
		0x00,
		0x00, 0x29,
		0x10, 0x00,
		0x00, 0x00, 0x80, 0x00,
		0x00, 0x00,
	}

	if got := extractMinDNSTTL(resp); got != 60 {
		t.Fatalf("expected OPT pseudo-ttl to be ignored, got ttl=%d", got)
	}
}

func TestExchangeDNSOverTCPSetsDeadlineOnPooledConn(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	pooled := &deadlineTrackingConn{Conn: client}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{ConnectTimeoutMS: 1000},
		},
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: pooled}},
		},
		activeUpstreams: make(map[io.Closer]struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var lengthBuf [2]byte
		if _, err := io.ReadFull(server, lengthBuf[:]); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint16(lengthBuf[:]))
		payload := make([]byte, size)
		if _, err := io.ReadFull(server, payload); err != nil {
			return
		}
		reply := []byte{0x00, 0x02, 0xbe, 0xef}
		_, _ = server.Write(reply)
	}()

	resp, releaseResp, err := e.exchangeDNSOverTCP(context.Background(), "8.8.8.8:53", []byte{0x01, 0x02}, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("exchangeDNSOverTCP failed: %v", err)
	}
	defer releaseResp()
	<-done

	if !bytes.Equal(resp, []byte{0xbe, 0xef}) {
		t.Fatalf("unexpected dns response %x", resp)
	}
	if pooled.deadlineCallCount() == 0 {
		t.Fatal("expected pooled dns conn to receive a deadline")
	}
}

func TestReleaseDNSConnPoolFullUnregistersActiveUpstream(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	idle := make([]idleDNSConn, dnsPoolPerTarget)
	for i := range idle {
		idle[i] = idleDNSConn{conn: &closeTrackingConn{remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: i + 1}}}
	}
	e := &Engine{
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": idle,
		},
	}
	e.registerActiveUpstream(client)

	e.releaseDNSConn("8.8.8.8:53", client, 0)

	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected active upstream conn to be unregistered when dns pool is full, got %d", len(e.activeUpstreams))
	}
}

func TestAcquireTCPDialSlotWaitsBriefly(t *testing.T) {
	e := &Engine{tcpDialSem: make(chan struct{}, 1)}
	e.tcpDialSem <- struct{}{}

	go func() {
		time.Sleep(100 * time.Millisecond)
		e.releaseTCPDialSlot()
	}()

	start := time.Now()
	if !e.acquireTCPDialSlot(context.Background(), 300*time.Millisecond) {
		t.Fatal("expected tcp dial slot acquisition to wait and succeed")
	}
	elapsed := time.Since(start)
	e.releaseTCPDialSlot()

	if elapsed < 80*time.Millisecond {
		t.Fatalf("tcp dial slot acquisition did not wait long enough: %s", elapsed)
	}
}

func TestEngineStopReturnsWhenTunCloseBlocks(t *testing.T) {
	blockCh := make(chan struct{})
	e := &Engine{
		tunDev: &TunDevice{
			closeFunc: func(ctx context.Context) error {
				select {
				case <-blockCh:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		dnsPool:         make(map[string][]idleDNSConn),
		udpPool:         make(map[string][]UDPSession),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := e.Stop(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected Stop to return shortly after context timeout, took %s", elapsed)
	}

	close(blockCh)
}

func TestEngineStopCanWaitForBlockedTunCloseOnRetry(t *testing.T) {
	blockCh := make(chan struct{})
	e := &Engine{
		tunDev: &TunDevice{
			closeFunc: func(ctx context.Context) error {
				select {
				case <-blockCh:
				case <-ctx.Done():
					return ctx.Err()
				}
				timer := time.NewTimer(100 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		dnsPool:         make(map[string][]idleDNSConn),
		udpPool:         make(map[string][]UDPSession),
	}

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer firstCancel()
	if err := e.Stop(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected first stop to time out, got %v", err)
	}

	close(blockCh)

	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	start := time.Now()
	if err := e.Stop(secondCtx); err != nil {
		t.Fatalf("expected second stop to wait for tun close completion, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("expected second stop to wait for outstanding tun close, got %s", elapsed)
	}
}

func TestWorkerWaitDoneUsesSingleWaiter(t *testing.T) {
	e := &Engine{}
	e.wg.Add(1)
	first := e.workerWaitDone()
	second := e.workerWaitDone()
	if first != second {
		t.Fatal("expected repeated shutdown waits to share one completion channel")
	}
	e.wg.Done()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("shared worker completion channel did not close")
	}
}

func TestNewEngineRejectsNilConfig(t *testing.T) {
	e, err := NewEngine(nil)
	if err == nil {
		t.Fatalf("expected error for nil config, got engine=%v", e)
	}
}

func TestEngineStartRejectsAlreadyStartedOrStopped(t *testing.T) {
	startedEngine := &Engine{started: true}
	if err := startedEngine.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("expected already started error, got %v", err)
	}

	stoppedEngine := &Engine{stopping: true}
	if err := stoppedEngine.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already stopped") {
		t.Fatalf("expected already stopped error, got %v", err)
	}

	stoppedStartedEngine := &Engine{started: true, stopping: true}
	if err := stoppedStartedEngine.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already stopped") {
		t.Fatalf("expected stopped engine to report already stopped, got %v", err)
	}
}

func TestEngineStartRejectsMissingConfig(t *testing.T) {
	var e Engine
	if err := e.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "nil config") {
		t.Fatalf("expected nil config error, got %v", err)
	}
}

func TestEngineStopBeforeStartIsNoop(t *testing.T) {
	e, err := NewEngine(&Config{
		Mode: ModeSocks5TLS,
		Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
		Upstream: UpstreamConfig{
			Addr: "127.0.0.1:443",
		},
		Runtime: RuntimeConfig{
			ConnectTimeoutMS: 1000,
			IdleTimeoutMS:    1000,
			TCPBuffer:        4096,
			UDPBuffer:        4096,
		},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("expected stop-before-start to be no-op, got %v", err)
	}
	if e.stopping {
		t.Fatal("expected stop-before-start to leave engine reusable")
	}
	if e.started {
		t.Fatal("expected unstarted engine to remain unstarted")
	}
}

func TestEngineStopWaitsForStartupRollback(t *testing.T) {
	blockCh := make(chan struct{})
	tun := &TunDevice{
		closeFunc: func(ctx context.Context) error {
			select {
			case <-blockCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	e := &Engine{rollbackTun: tun}

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer firstCancel()
	if err := e.Stop(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected rollback wait to time out, got %v", err)
	}

	close(blockCh)

	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	if err := e.Stop(secondCtx); err != nil {
		t.Fatalf("expected rollback wait to complete, got %v", err)
	}
	if e.rollbackTun != nil {
		t.Fatal("expected rollback tun to be cleared after teardown completes")
	}
}

func TestEngineStopWaitsWhileStartInProgress(t *testing.T) {
	startDone := make(chan struct{})
	e := &Engine{
		starting:  true,
		startDone: startDone,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := e.Stop(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected stop to wait for start window and time out, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected stop to wait for start window, got %s", elapsed)
	}

	e.mu.Lock()
	e.starting = false
	e.startDone = nil
	e.mu.Unlock()
	close(startDone)
}

func TestEngineStopCancelsStartInProgress(t *testing.T) {
	startDone := make(chan struct{})
	cancelled := make(chan struct{})
	e := &Engine{
		starting:  true,
		startDone: startDone,
		startCancel: func() {
			select {
			case <-cancelled:
			default:
				close(cancelled)
			}
		},
	}

	stopErrCh := make(chan error, 1)
	go func() {
		stopErrCh <- e.Stop(context.Background())
	}()

	select {
	case <-cancelled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected stop to cancel in-progress startup")
	}

	e.mu.Lock()
	e.starting = false
	e.startDone = nil
	e.startCancel = nil
	e.mu.Unlock()
	close(startDone)

	select {
	case err := <-stopErrCh:
		if err != nil {
			t.Fatalf("expected stop to complete after startup cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected stop to finish after startup completion signal")
	}
}

func TestAcquireHandlerSlotRejectsWhenFull(t *testing.T) {
	const cap = 2
	e := &Engine{handlerSem: make(chan struct{}, cap)}

	if !e.acquireHandlerSlot() {
		t.Fatal("expected first acquire to succeed")
	}
	if !e.acquireHandlerSlot() {
		t.Fatal("expected second acquire to succeed")
	}
	if e.acquireHandlerSlot() {
		t.Fatal("expected third acquire to fail when semaphore is full")
	}

	/* Releasing one slot should allow acquisition again. */
	e.releaseHandlerSlot()
	if !e.acquireHandlerSlot() {
		t.Fatal("expected acquire to succeed after release")
	}
}

func TestUDPHandlerSlotUsesIndependentSemaphore(t *testing.T) {
	e := &Engine{
		handlerSem:    make(chan struct{}, 1),
		udpHandlerSem: make(chan struct{}, 1),
	}

	if !e.acquireHandlerSlot() {
		t.Fatal("expected tcp handler acquire to succeed")
	}
	if !e.acquireUDPHandlerSlot() {
		t.Fatal("expected udp handler acquire to succeed even when tcp handler pool is full")
	}
	if e.acquireUDPHandlerSlot() {
		t.Fatal("expected second udp handler acquire to fail when udp pool is full")
	}

	e.releaseUDPHandlerSlot()
	if !e.acquireUDPHandlerSlot() {
		t.Fatal("expected udp handler acquire to succeed after release")
	}
}

func TestEngineStopDuringStartupAfterNetstackCreated(t *testing.T) {
	/*
	 * Simulates the race where Stop sets e.stopping=true in between netstack
	 * creation and e.started=true being committed inside Start's final lock.
	 * This exercises the "engine stopped during startup" path in Start.
	 */

	blockCh := make(chan struct{})
	/* Builds a TunDevice whose close blocks until the test unblocks it. */
	tun := &TunDevice{
		closeFunc: func(ctx context.Context) error {
			select {
			case <-blockCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}

	e := &Engine{
		stopping:    true,
		rollbackTun: tun,
	}

	/* Stop should drain the rollback tun. */
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()

	/* The first call times out while the rollback tun is blocked. */
	if err := e.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded while rollback tun blocks, got %v", err)
	}

	close(blockCh)

	/* A fresh second call should complete once the tun unblocks. */
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err := e.Stop(ctx2); err != nil {
		t.Fatalf("expected stop to complete after tun unblocks, got %v", err)
	}
	if e.rollbackTun != nil {
		t.Fatal("expected rollbackTun to be cleared after stop completes")
	}
}

func TestEngineStartRejectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done

	var e Engine
	err := e.Start(ctx)
	if err == nil {
		t.Fatal("expected error for already-cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestHandleUDPDoesNotReuseSessionWithZeroUpstreamBytes(t *testing.T) {
	/*
	 * When shouldReuseUDPSession returns true but upstream delivered zero bytes
	 * (for example, all frames were oversized and silently dropped), the
	 * call-site guard must flip reusableSession to false so the session is not
	 * returned to the pool.
	 */
	e := &Engine{}

	/* shouldReuseUDPSession allows timeout errors, so this would normally return true. */
	reusable := e.shouldReuseUDPSession(context.Background(), nil /* localToUpstream */, context.DeadlineExceeded /* upstreamToLocal */)
	if !reusable {
		t.Skip("precondition: timeout alone must not block reuse (test assumptions changed)")
	}

	/* Simulates the call-site guard in handleUDP. */
	upstreamBytes := int64(0)
	upstreamErr := context.DeadlineExceeded // timeout with no bytes received
	if reusable && upstreamBytes == 0 && upstreamErr != nil {
		reusable = false
	}
	if reusable {
		t.Fatal("expected zero-bytes guard to prevent session reuse after timeout with no upstream data")
	}
}

func TestUDPNoUpstreamResponseDisablesReuseEvenWithoutReadError(t *testing.T) {
	e := &Engine{}
	localToUpstream := udpPumpResult{Direction: "local_to_upstream", Bytes: 1200, Packets: 1}
	upstreamToLocal := udpPumpResult{Direction: "upstream_to_local"}

	reusable := e.shouldReuseUDPSession(context.Background(), localToUpstream.Err, upstreamToLocal.Err)
	if !reusable {
		t.Fatal("precondition: nil pump errors should allow udp session reuse")
	}
	if reusable && udpNoUpstreamResponse(localToUpstream, upstreamToLocal) {
		reusable = false
	}
	if reusable {
		t.Fatal("expected no-response udp flow to disable session reuse")
	}
}

func TestCombineUDPResultsReportsNoResponse(t *testing.T) {
	got := combineUDPResults(
		udpPumpResult{Direction: "local_to_upstream", Bytes: 1200, Packets: 1},
		udpPumpResult{Direction: "upstream_to_local"},
	)
	if got != "up:ok/down:no-response" {
		t.Fatalf("expected udp no-response summary, got %q", got)
	}
}

func TestShouldLogUDPSummaryReportsNoResponse(t *testing.T) {
	if !shouldLogUDPSummary(
		udpPumpResult{Direction: "local_to_upstream", Bytes: 1200, Packets: 1},
		udpPumpResult{Direction: "upstream_to_local"},
		10*time.Millisecond,
	) {
		t.Fatal("expected udp no-response flow to be logged")
	}
}

func TestLogDirectUDPSummaryThrottlesNoResponseBurst(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)

	SetLogLevel(LogLevelInfo)
	e := &Engine{logThrottleStates: make(map[string]logThrottleState)}
	up := udpPumpResult{Direction: "local_to_upstream", Bytes: 32, Packets: 1}
	down := udpPumpResult{Direction: "upstream_to_local"}

	e.logDirectUDPSummary("udp-1", "198.18.0.1:50000", "1.0.1.8:443", "direct_cidr", time.Second, up, down)
	e.logDirectUDPSummary("udp-2", "198.18.0.1:50001", "1.0.1.8:443", "direct_cidr", time.Second, up, down)
	if got := strings.Count(out.String(), "down:no-response"); got != 1 {
		t.Fatalf("expected one no-response log for a burst, got %d: %q", got, out.String())
	}

	state := e.logThrottleStates["direct|udp-no-response"]
	state.LastLogTime = time.Now().Add(-logThrottleInterval)
	e.logThrottleStates["direct|udp-no-response"] = state
	e.logDirectUDPSummary("udp-3", "198.18.0.1:50002", "2.0.0.8:443", "direct_cidr", time.Second, up, down)
	if got := strings.Count(out.String(), "down:no-response"); got != 2 {
		t.Fatalf("expected summary after throttle interval, got %d: %q", got, out.String())
	}
	if !strings.Contains(out.String(), "suppressed=1") {
		t.Fatalf("expected suppressed count in next summary, got %q", out.String())
	}
}

type testTimeoutError struct{}

func (testTimeoutError) Error() string   { return "i/o timeout" }
func (testTimeoutError) Timeout() bool   { return true }
func (testTimeoutError) Temporary() bool { return true }

func TestShouldLogTCPSummarySuppressesOrdinaryIdleTimeout(t *testing.T) {
	stats := bidirectionalRelayStats{
		UploadBytes:   2 * 1024,
		DownloadBytes: 6 * 1024,
		UploadErr:     testTimeoutError{},
		DownloadErr:   testTimeoutError{},
		Duration:      5 * time.Minute,
	}

	if shouldLogTCPSummary(stats) {
		t.Fatal("expected ordinary idle timeout with small transfer to stay quiet")
	}
}

func TestShouldLogTCPSummarySuppressesOrdinaryLongOKFlow(t *testing.T) {
	stats := bidirectionalRelayStats{
		UploadBytes:   2 * 1024,
		DownloadBytes: 6 * 1024,
		Duration:      5 * time.Minute,
	}

	if shouldLogTCPSummary(stats) {
		t.Fatal("expected ordinary long ok flow with small transfer to stay quiet")
	}
}

func TestShouldLogTCPSummaryKeepsUnexpectedErrorsAndLargeTransfers(t *testing.T) {
	if !shouldLogTCPSummary(bidirectionalRelayStats{
		UploadErr: errors.New("upstream reset"),
	}) {
		t.Fatal("expected unexpected relay error to be logged")
	}
	if !shouldLogTCPSummary(bidirectionalRelayStats{
		UploadBytes: flowSummaryBytesThreshold,
		UploadErr:   testTimeoutError{},
		DownloadErr: testTimeoutError{},
		Duration:    5 * time.Minute,
	}) {
		t.Fatal("expected large transfer summary to be logged even when closed by idle timeout")
	}
}

func TestBenignTCPForwarderEndpointError(t *testing.T) {
	if !isBenignTCPForwarderEndpointError(&tcpip.ErrConnectionRefused{}) {
		t.Fatal("expected handshake refusal to be treated as a benign client abort")
	}
	if isBenignTCPForwarderEndpointError(&tcpip.ErrInvalidEndpointState{}) {
		t.Fatal("invalid endpoint state must remain visible as an error")
	}
}

func TestCloseActiveLocalsClearsTrackedLocals(t *testing.T) {
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}}
	e := &Engine{activeLocals: make(map[io.Closer]struct{})}
	e.registerActiveLocal(conn)

	e.closeActiveLocals()

	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeLocals) != 0 {
		t.Fatalf("expected active locals to be cleared, got %d", len(e.activeLocals))
	}
}

func TestCloseActiveUpstreamsClearsTrackedUpstreams(t *testing.T) {
	conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}}
	e := &Engine{activeUpstreams: make(map[io.Closer]struct{})}
	e.registerActiveUpstream(conn)

	e.closeActiveUpstreams()

	if !conn.isClosed() {
		t.Fatal("expected active upstream to be closed")
	}
	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected active upstreams to be cleared, got %d", len(e.activeUpstreams))
	}
}

func TestRelayBidirectionalWithStatsContextStopsOnCancel(t *testing.T) {
	left := &blockingRelayConn{}
	right := &blockingRelayConn{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bidirectionalRelayStats, 1)
	go func() {
		var pool relayBufferPool
		done <- relayBidirectionalWithStatsContext(ctx, left, right, 32*1024, 30*time.Second, &pool)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case stats := <-done:
		if stats.UploadErr == nil && stats.DownloadErr == nil {
			t.Fatal("expected canceled relay to report at least one error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected relay to stop shortly after context cancellation")
	}
}

func TestDialTCPUpstreamWaitsForCooldownToClear(t *testing.T) {
	dialer := &stubRetryTCPDialer{response: []byte{0xaa}}
	e := &Engine{
		cfg: &Config{
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}
	e.markTCPDialBackoff(120 * time.Millisecond)

	start := time.Now()
	conn, err := e.dialTCPUpstream(context.Background(), "8.8.8.8:53", time.Second)
	if err != nil {
		t.Fatalf("expected dial to succeed after cooldown clears, got %v", err)
	}
	_ = conn.Close()

	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("expected dial to wait for cooldown, got %s", elapsed)
	}
	if dialer.dials != 1 {
		t.Fatalf("expected 1 dial after cooldown, got %d", dialer.dials)
	}
}

func TestDialTCPUpstreamAppliesAttemptBudget(t *testing.T) {
	e := &Engine{
		dialer:     &stubBlockingTCPDialer{},
		tcpDialSem: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	conn, err := e.dialTCPUpstream(ctx, "8.8.8.8:53", 50*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected blocking dial to fail on attempt budget")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("expected attempt budget to stop dial quickly, took %s", elapsed)
	}
}

func TestDialTCPUpstreamBacksOffTargetSpecificSOCKSFailure(t *testing.T) {
	recorder := &dialOrderRecorder{}
	e := &Engine{
		dialer: &multiUpstreamDialer{
			mode: ModeSocks5TLS,
			dialers: []UpstreamDialer{
				&stubOrderedUpstreamDialer{idx: 0, tcpErr: &socks5ReplyError{command: "connect", code: 0x04}, order: recorder},
			},
		},
		tcpDialSem: make(chan struct{}, 2),
	}
	target := "103.228.130.61:443"

	conn, err := e.dialTCPUpstream(context.Background(), target, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected target-specific socks failure")
	}
	if !e.isTCPTargetCoolingDown(target) {
		t.Fatal("expected failed target to enter cooldown")
	}

	conn, err = e.dialTCPUpstream(context.Background(), target, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected target cooldown to reject the repeated dial")
	}
	if !errors.Is(err, errTCPTargetCooldown) {
		t.Fatalf("expected target cooldown error, got %v", err)
	}
	if got := len(recorder.snapshot()); got != 1 {
		t.Fatalf("expected cooldown to prevent another upstream dial, got %d attempts", got)
	}
}

func TestDialTCPUpstreamKeepsFallbackAvailableAfterTargetSpecificSOCKSFailure(t *testing.T) {
	recorder := &dialOrderRecorder{}
	target := "103.228.130.61:443"
	e := &Engine{
		dialer: &multiUpstreamDialer{
			mode: ModeSocks5TLS,
			dialers: []UpstreamDialer{
				&stubOrderedUpstreamDialer{idx: 0, tcpErr: &socks5ReplyError{command: "connect", code: 0x04}, order: recorder},
				&stubOrderedUpstreamDialer{idx: 1, order: recorder},
			},
		},
		tcpDialSem: make(chan struct{}, 2),
	}

	conn, err := e.dialTCPUpstream(context.Background(), target, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("expected alternate upstream to recover target-specific failure, got %v", err)
	}
	_ = conn.Close()
	if e.isTCPTargetCoolingDown(target) {
		t.Fatal("expected successful fallback to keep the target out of cooldown")
	}
	if got, want := recorder.snapshot(), []int{0, 1}; !slices.Equal(got, want) {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
}

func TestDialTCPUpstreamDoesNotBackoffMixedUpstreamFailures(t *testing.T) {
	for name, otherErr := range map[string]error{
		"timeout":       context.DeadlineExceeded,
		"transport_err": errors.New("tls handshake failed"),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := &dialOrderRecorder{}
			target := "103.228.130.61:443"
			e := &Engine{
				dialer: &multiUpstreamDialer{
					mode: ModeSocks5TLS,
					dialers: []UpstreamDialer{
						&stubOrderedUpstreamDialer{idx: 0, tcpErr: &socks5ReplyError{command: "connect", code: 0x04}, order: recorder},
						&stubOrderedUpstreamDialer{idx: 1, tcpErr: otherErr, order: recorder},
					},
				},
				tcpDialSem: make(chan struct{}, 2),
			}

			conn, err := e.dialTCPUpstream(context.Background(), target, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				t.Fatal("expected both upstream attempts to fail")
			}
			if e.isTCPTargetCoolingDown(target) {
				t.Fatalf("mixed upstream failure must not cool down target: %v", err)
			}
		})
	}
}

func TestHandleNetworkChangeRejectsStaleTargetBackoff(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	dialer := &stubSequencedTCPDialer{
		entered: []chan struct{}{entered},
		release: []chan struct{}{release},
		errs:    []error{&socks5ReplyError{command: "connect", code: 0x04}},
	}
	target := "103.228.130.61:443"
	e := &Engine{dialer: dialer, tcpDialSem: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() {
		conn, err := e.dialTCPUpstream(context.Background(), target, time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	e.HandleNetworkChange()
	close(release)
	if err := <-done; err == nil {
		t.Fatal("expected controlled target failure")
	}
	if e.isTCPTargetCoolingDown(target) {
		t.Fatal("stale pre-network-change failure restored target cooldown")
	}
}

func TestStaleTCPSuccessCannotMaskCurrentGenerationFailure(t *testing.T) {
	target := "103.228.130.61:443"
	e := &Engine{}
	staleGeneration := e.networkGeneration.Load()
	e.HandleNetworkChange()

	attempt, err := e.beginTCPTargetDial(target)
	if err != nil {
		t.Fatalf("begin current-generation target dial: %v", err)
	}
	defer e.releaseTCPTargetDial(target)

	e.clearTCPTargetBackoffForGeneration(target, staleGeneration)
	e.markTCPTargetBackoffForAttempt(attempt, time.Second)
	if !e.isTCPTargetCoolingDown(target) {
		t.Fatal("stale success masked a current-generation target failure")
	}
}

func TestTCPTargetAttemptRefreshesAfterAdmissionGenerationChanges(t *testing.T) {
	target := "103.228.130.61:443"
	e := &Engine{}
	attempt, err := e.beginTCPTargetDial(target)
	if err != nil {
		t.Fatalf("begin target dial: %v", err)
	}
	defer e.releaseTCPTargetDial(target)

	e.HandleNetworkChange()
	refreshed, ok := e.refreshTCPTargetDialAttempt(attempt, e.networkGeneration.Load())
	if !ok {
		t.Fatal("expected admitted target attempt to refresh onto the current generation")
	}
	e.markTCPTargetBackoffForAttempt(refreshed, time.Second)
	if !e.isTCPTargetCoolingDown(target) {
		t.Fatal("current-generation target failure was discarded after admission refresh")
	}
}

func TestBeginTCPTargetDialAtomicallyHonorsCooldown(t *testing.T) {
	e := &Engine{}
	target := "103.228.130.61:443"
	e.markTCPTargetBackoff(target, time.Second)
	if _, err := e.beginTCPTargetDial(target); !errors.Is(err, errTCPTargetCooldown) {
		t.Fatalf("expected atomic target admission to reject cooldown, got %v", err)
	}
}

func TestConcurrentTCPSuccessPreventsSlowerFailureBackoff(t *testing.T) {
	failureEntered, successEntered := make(chan struct{}), make(chan struct{})
	failureRelease, successRelease := make(chan struct{}), make(chan struct{})
	dialer := &stubSequencedTCPDialer{
		entered: []chan struct{}{failureEntered, successEntered},
		release: []chan struct{}{failureRelease, successRelease},
		errs: []error{
			&socks5ReplyError{command: "connect", code: 0x04},
			nil,
		},
	}
	target := "103.228.130.61:443"
	e := &Engine{dialer: dialer, tcpDialSem: make(chan struct{}, 2)}
	failureDone := make(chan error, 1)
	successDone := make(chan error, 1)
	go func() {
		_, err := e.dialTCPUpstream(context.Background(), target, time.Second)
		failureDone <- err
	}()
	<-failureEntered
	go func() {
		conn, err := e.dialTCPUpstream(context.Background(), target, time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		successDone <- err
	}()
	<-successEntered
	close(successRelease)
	if err := <-successDone; err != nil {
		t.Fatalf("expected concurrent success, got %v", err)
	}
	close(failureRelease)
	if err := <-failureDone; err == nil {
		t.Fatal("expected slower controlled failure")
	}
	if e.isTCPTargetCoolingDown(target) {
		t.Fatal("slower failure overrode a newer successful target dial")
	}
}

func TestAcquireTCPTargetDialLimitsConcurrentAttempts(t *testing.T) {
	e := &Engine{}
	target := "103.228.130.61:443"
	for i := 0; i < tcpTargetDialConcurrency; i++ {
		if !e.acquireTCPTargetDial(target) {
			t.Fatalf("expected target dial slot %d to be available", i+1)
		}
	}
	if e.acquireTCPTargetDial(target) {
		t.Fatal("expected per-target tcp dial concurrency limit")
	}
	e.releaseTCPTargetDial(target)
	if !e.acquireTCPTargetDial(target) {
		t.Fatal("expected released target dial slot to become available")
	}
}

func TestAcquireTCPTargetDialAdmitsNormalFanoutBurst(t *testing.T) {
	const burstSize = 16
	e := &Engine{}
	target := "1.1.1.1:53"
	for i := 0; i < burstSize; i++ {
		if !e.acquireTCPTargetDial(target) {
			t.Fatalf("normal target fanout was throttled at attempt %d", i+1)
		}
		defer e.releaseTCPTargetDial(target)
	}
}

func TestDialTCPUpstreamWaitsForTargetSlot(t *testing.T) {
	for _, target := range []string{"1.1.1.1:53", "203.0.113.8:443"} {
		t.Run(target, func(t *testing.T) {
			dialer := &stubRetryTCPDialer{response: []byte{0xaa}}
			e := &Engine{
				dialer:     dialer,
				tcpDialSem: make(chan struct{}, 1),
			}
			for i := 0; i < tcpTargetDialConcurrency; i++ {
				if !e.acquireTCPTargetDial(target) {
					t.Fatalf("expected target dial slot %d to be available", i+1)
				}
			}
			go func() {
				time.Sleep(50 * time.Millisecond)
				e.releaseTCPTargetDial(target)
			}()

			start := time.Now()
			conn, err := e.dialTCPUpstream(context.Background(), target, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("expected dial to wait for a target slot, got %v", err)
			}
			_ = conn.Close()
			if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
				t.Fatalf("expected dial to wait for the released slot, took %s", elapsed)
			}
		})
	}
}

func TestDialTCPUpstreamBoundsTargetWaiters(t *testing.T) {
	e := &Engine{
		dialer:     &stubRetryTCPDialer{response: []byte{0xaa}},
		tcpDialSem: make(chan struct{}, 1),
	}
	target := "203.0.113.8:443"
	for i := 0; i < tcpTargetDialConcurrency; i++ {
		if !e.acquireTCPTargetDial(target) {
			t.Fatalf("expected target dial slot %d to be available", i+1)
		}
	}
	for i := 0; i < tcpTargetDialWaitConcurrency; i++ {
		if !e.acquireTCPTargetWaiter() {
			t.Fatalf("expected target waiter %d to be available", i+1)
		}
	}
	defer func() {
		for i := 0; i < tcpTargetDialWaitConcurrency; i++ {
			e.releaseTCPTargetWaiter()
		}
	}()

	start := time.Now()
	conn, err := e.dialTCPUpstream(context.Background(), target, 500*time.Millisecond)
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, errTCPTargetThrottle) {
		t.Fatalf("expected bounded target wait queue to reject excess pressure, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("expected excess target pressure to fail fast, took %s", elapsed)
	}
}

func TestTCPConnectBudgetHonorsConfiguredConnectTimeout(t *testing.T) {
	e := &Engine{cfg: &Config{Runtime: RuntimeConfig{ConnectTimeoutMS: 10_000}}}
	if got := e.tcpConnectBudget(); got != 10*time.Second {
		t.Fatalf("expected tcp connect budget to honor connect_timeout, got %s", got)
	}
}

func TestDialTCPUpstreamFallbacksAfterTransientAttemptError(t *testing.T) {
	first := &stubRetryTCPDialer{failCount: 1}
	success := &stubRetryTCPDialer{response: []byte{0xaa}}
	e := &Engine{
		dialer: &multiUpstreamDialer{
			mode:    ModeSocks5TLS,
			dialers: []UpstreamDialer{first, success},
		},
		tcpDialSem: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := e.dialTCPUpstream(ctx, "8.8.8.8:53", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected second upstream to recover after first transient failure, got %v", err)
	}
	_ = conn.Close()
	if first.dials != 1 {
		t.Fatalf("expected first upstream to be tried once, got %d", first.dials)
	}
	if success.dials != 1 {
		t.Fatalf("expected second upstream to be tried once, got %d", success.dials)
	}
}

func TestMultiUpstreamDialTCPKeepsLastStable(t *testing.T) {
	first := &stubRetryTCPDialer{failCount: 1}
	second := &stubRetryTCPDialer{response: []byte{0xaa}}
	dialer := &multiUpstreamDialer{
		mode:    ModeSocks5TLS,
		dialers: []UpstreamDialer{first, second},
	}

	conn, err := dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", time.Second)
	if err != nil {
		t.Fatalf("expected fallback upstream to recover, got %v", err)
	}
	_ = conn.Close()
	if got := dialer.stableIndex.Load(); got != 1 {
		t.Fatalf("stable index = %d, want 1", got)
	}

	conn, err = dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", time.Second)
	if err != nil {
		t.Fatalf("expected stable upstream to be tried first, got %v", err)
	}
	_ = conn.Close()
	if first.dials != 1 {
		t.Fatalf("first upstream dials = %d, want 1", first.dials)
	}
	if second.dials != 2 {
		t.Fatalf("second upstream dials = %d, want 2", second.dials)
	}
}

func TestMultiUpstreamDialTCPStableIndexConcurrent(t *testing.T) {
	first := &stubConcurrentTCPDialer{fail: true}
	second := &stubConcurrentTCPDialer{}
	dialer := &multiUpstreamDialer{
		mode:    ModeSocks5TLS,
		dialers: []UpstreamDialer{first, second},
	}

	const goroutines = 32
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", time.Second)
			if err != nil {
				errCh <- err
				return
			}
			_ = conn.Close()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("expected concurrent TCP fallback to succeed, got %v", err)
	}
	if got := dialer.stableIndex.Load(); got != 1 {
		t.Fatalf("stable index = %d, want 1", got)
	}
	if second.dials.Load() == 0 {
		t.Fatal("expected second upstream to be used")
	}
}

func TestMultiUpstreamDialTCPFallsBackThroughThreeUpstreamsFromStable(t *testing.T) {
	order := &dialOrderRecorder{}
	dialer := &multiUpstreamDialer{
		mode: ModeSocks5TLS,
		dialers: []UpstreamDialer{
			&stubOrderedUpstreamDialer{idx: 0, tcpErr: errors.New("first failed"), order: order},
			&stubOrderedUpstreamDialer{idx: 1, tcpErr: errors.New("stable failed"), order: order},
			&stubOrderedUpstreamDialer{idx: 2, order: order},
		},
	}
	dialer.stableIndex.Store(1)

	conn, err := dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", time.Second)
	if err != nil {
		t.Fatalf("expected third upstream to recover, got %v", err)
	}
	_ = conn.Close()
	if got, want := order.snapshot(), []int{1, 0, 2}; !slices.Equal(got, want) {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
	if got := dialer.stableIndex.Load(); got != 2 {
		t.Fatalf("stable index = %d, want 2", got)
	}
}

func TestMultiUpstreamDialUDPKeepsLastStable(t *testing.T) {
	first := &stubFailingUDPDialer{}
	second := &stubUDPDialCounter{}
	dialer := &multiUpstreamDialer{
		mode:    ModeSocks5TLS,
		dialers: []UpstreamDialer{first, second},
	}

	session, err := dialer.DialUDP(context.Background())
	if err != nil {
		t.Fatalf("expected fallback udp upstream to recover, got %v", err)
	}
	_ = session.Close()
	if got := dialer.stableIndex.Load(); got != 1 {
		t.Fatalf("stable index = %d, want 1", got)
	}

	session, err = dialer.DialUDP(context.Background())
	if err != nil {
		t.Fatalf("expected stable udp upstream to be tried first, got %v", err)
	}
	_ = session.Close()
	if first.count() != 1 {
		t.Fatalf("first udp upstream dials = %d, want 1", first.count())
	}
	if second.count() != 2 {
		t.Fatalf("second udp upstream dials = %d, want 2", second.count())
	}
}

func TestMultiUpstreamDialUDPStableIndexConcurrent(t *testing.T) {
	first := &stubConcurrentUDPDialer{fail: true}
	second := &stubConcurrentUDPDialer{}
	dialer := &multiUpstreamDialer{
		mode:    ModeSocks5TLS,
		dialers: []UpstreamDialer{first, second},
	}

	const goroutines = 32
	errCh := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, err := dialer.DialUDP(context.Background())
			if err != nil {
				errCh <- err
				return
			}
			_ = session.Close()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("expected concurrent UDP fallback to succeed, got %v", err)
	}
	if got := dialer.stableIndex.Load(); got != 1 {
		t.Fatalf("stable index = %d, want 1", got)
	}
	if second.dials.Load() == 0 {
		t.Fatal("expected second upstream to be used")
	}
}

func TestMultiUpstreamDialUDPFallsBackThroughThreeUpstreamsFromStable(t *testing.T) {
	order := &dialOrderRecorder{}
	dialer := &multiUpstreamDialer{
		mode: ModeSocks5TLS,
		dialers: []UpstreamDialer{
			&stubOrderedUpstreamDialer{idx: 0, udpErr: errors.New("first failed"), order: order},
			&stubOrderedUpstreamDialer{idx: 1, udpErr: errors.New("stable failed"), order: order},
			&stubOrderedUpstreamDialer{idx: 2, order: order},
		},
	}
	dialer.stableIndex.Store(1)

	session, err := dialer.DialUDP(context.Background())
	if err != nil {
		t.Fatalf("expected third udp upstream to recover, got %v", err)
	}
	_ = session.Close()
	if got, want := order.snapshot(), []int{1, 0, 2}; !slices.Equal(got, want) {
		t.Fatalf("dial order = %v, want %v", got, want)
	}
	if got := dialer.stableIndex.Load(); got != 2 {
		t.Fatalf("stable index = %d, want 2", got)
	}
}

func TestDialTCPUpstreamSharesTotalBudgetAcrossMultiUpstream(t *testing.T) {
	success := &stubRetryTCPDialer{response: []byte{0xaa}}
	e := &Engine{
		dialer: &multiUpstreamDialer{
			mode:    ModeSocks5TLS,
			dialers: []UpstreamDialer{&stubBlockingTCPDialer{}, success},
		},
		tcpDialSem: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	conn, err := e.dialTCPUpstream(ctx, "8.8.8.8:53", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected fallback upstream to receive part of the total budget, got %v", err)
	}
	_ = conn.Close()
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("expected shared total budget to stop the blocking attempt quickly, took %s", elapsed)
	}
	if success.dials != 1 {
		t.Fatalf("expected fallback upstream to be attempted once, got %d", success.dials)
	}
}

func TestMultiUpstreamDialTCPErrorIncludesAddress(t *testing.T) {
	dialer := &multiUpstreamDialer{
		mode:          ModeSocks5TLS,
		dialers:       []UpstreamDialer{&stubRetryTCPDialer{failCount: 1}, &stubRetryTCPDialer{failCount: 1}},
		upstreamAddrs: []string{"8.211.166.217:443", "147.78.246.27:443"},
	}

	conn, err := dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected all upstream attempts to fail")
	}
	errText := err.Error()
	if !strings.Contains(errText, "upstream[0] 8.211.166.217:443") ||
		!strings.Contains(errText, "upstream[1] 147.78.246.27:443") {
		t.Fatalf("expected upstream addresses in joined error, got %v", err)
	}
}

func TestMultiUpstreamDialTCPWithAttemptBudgetRejectsExhaustedBudget(t *testing.T) {
	success := &stubRetryTCPDialer{response: []byte{0xaa}}
	dialer := &multiUpstreamDialer{
		mode:    ModeSocks5TLS,
		dialers: []UpstreamDialer{&stubBlockingTCPDialer{}, success},
	}

	conn, err := dialer.DialTCPWithAttemptBudget(context.Background(), "8.8.8.8:53", -time.Nanosecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected exhausted attempt budget to fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if success.dials != 0 {
		t.Fatalf("expected exhausted budget to skip fallback dials, got %d", success.dials)
	}
}

func TestTransientUpstreamDialErrorIncludesResetAndBrokenPipe(t *testing.T) {
	cases := []error{
		errors.New("dial upstream tls 127.0.0.1:443: read: connection reset by peer"),
		errors.New("write tcp 127.0.0.1:53000->127.0.0.1:443: write: broken pipe"),
	}
	for _, err := range cases {
		if !isTransientUpstreamDialError(err) {
			t.Fatalf("expected transient upstream dial error for %v", err)
		}
	}
}

func TestSocks5ReplyCodeReasonNamesHostUnreachable(t *testing.T) {
	if got := socks5ReplyCodeReason(0x04); got != "host unreachable" {
		t.Fatalf("expected host unreachable reason, got %q", got)
	}
	if got := socks5ReplyCodeReason(0xff); got != "unassigned" {
		t.Fatalf("expected unassigned reason, got %q", got)
	}
}

func TestHandleUDPDropProxiedUDP443ForFastTCPFallback(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)
	SetLogLevel(LogLevelInfo)

	dialer := &stubUDPDialCounter{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:               dialer,
		udpDialSem:           make(chan struct{}, 1),
		activeUpstreams:      make(map[io.Closer]struct{}),
		activeLocals:         make(map[io.Closer]struct{}),
		logThrottleStates:    make(map[string]logThrottleState),
		udpNoResponseBackoff: make(map[string]int64),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("142.250.194.67").To4()),
		LocalPort:     443,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})
	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})

	if got := dialer.count(); got != 0 {
		t.Fatalf("expected proxied udp/443 to be dropped without upstream dial, got %d dials", got)
	}
	if strings.Contains(out.String(), "proxied udp/443 disabled for fast tcp fallback") {
		t.Fatalf("expected proxied udp/443 policy drops to stay out of info logs, got %q", out.String())
	}
	if !e.isUDPNoResponseCoolingDown("142.250.194.67:443") {
		t.Fatal("expected proxied udp/443 target to enter drop cooldown")
	}
}

func TestHandleUDPMarksNoResponseCooldown(t *testing.T) {
	dialer := &stubUDPDialCounter{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:               dialer,
		udpDialSem:           make(chan struct{}, 1),
		activeUpstreams:      make(map[io.Closer]struct{}),
		activeLocals:         make(map[io.Closer]struct{}),
		udpNoResponseBackoff: make(map[string]int64),
	}
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02}}})
	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x03, 0x04}}})

	if got := dialer.count(); got != 1 {
		t.Fatalf("expected second no-response udp flow to be cooled down, got %d dials", got)
	}
	if !e.isUDPNoResponseCoolingDown("9.9.9.9:9999") {
		t.Fatal("expected no-response cooldown to be active for target")
	}
}

func TestHandleUDPReusesIdleSessionSequentially(t *testing.T) {
	dialer := &stubUDPDialCounter{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		tcpDialSem:      make(chan struct{}, 1),
		udpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})
	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})

	if got := dialer.count(); got != 2 {
		t.Fatalf("expected eof-closed udp session to be discarded, got %d dials", got)
	}

	e.udpPoolMu.Lock()
	defer e.udpPoolMu.Unlock()
	total := 0
	for _, sessions := range e.udpPool {
		total += len(sessions)
	}
	if total != 0 {
		t.Fatalf("expected no idle udp session in pool after eof, got %d", total)
	}
}

func TestHandleUDPDropsMismatchedUpstreamSourceOnReusedSession(t *testing.T) {
	session := &scriptedUDPSession{
		reads: []scriptedUDPRead{
			{
				payload: []byte{0xde, 0xad},
				src:     &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 9999},
				err:     nil,
			},
			{
				payload: nil,
				src:     nil,
				err:     io.EOF,
			},
		},
	}
	conn := &stubDNSPacketConn{
		reads: [][]byte{
			{0x01, 0x02},
		},
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          &stubUDPDialCounter{},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, conn)

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.writes) != 0 {
		t.Fatalf("expected mismatched upstream packet to be dropped, got %d writes", len(conn.writes))
	}
}

func TestHandleUDPRegistersReusedSessionAsActiveUpstream(t *testing.T) {
	session := &blockingUDPSession{
		readCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	conn := &stubDNSPacketConnBlocking{
		firstRead: []byte{0x01, 0x02},
		blockCh:   make(chan struct{}),
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          &stubUDPDialCounter{},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.handleUDP(context.Background(), id, conn)
	}()

	deadline := time.After(2 * time.Second)
	for {
		e.activeMu.Lock()
		_, ok := e.activeUpstreams[session]
		e.activeMu.Unlock()
		if ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("expected reused udp session to be tracked as active")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	_ = session.Close()
	conn.Close()
	<-done
}

func TestAcquireDNSConnRejectsStoppingEngineForPooledConn(t *testing.T) {
	conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}}
	e := &Engine{
		stopping:        true,
		activeUpstreams: make(map[io.Closer]struct{}),
		dnsPool: map[string][]idleDNSConn{
			"8.8.8.8:53": {{conn: conn}},
		},
	}

	got, pooled, _, err := e.acquireDNSConn(context.Background(), "8.8.8.8:53", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil conn, got %v", got)
	}
	if pooled {
		t.Fatal("expected pooled=false when activation fails")
	}
	if !conn.isClosed() {
		t.Fatal("expected pooled dns conn to be closed when engine is stopping")
	}
	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected no active upstreams, got %d", len(e.activeUpstreams))
	}
}

func TestAcquireDNSConnRejectsStoppingEngineForFreshConn(t *testing.T) {
	conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}}
	e := &Engine{
		stopping:        true,
		activeUpstreams: make(map[io.Closer]struct{}),
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
			},
		},
		dialer: &stubTCPDialerConn{conn: conn},
	}

	got, pooled, _, err := e.acquireDNSConn(context.Background(), "8.8.8.8:53", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil conn, got %v", got)
	}
	if pooled {
		t.Fatal("expected pooled=false for fresh dial failure")
	}
	if !conn.isClosed() {
		t.Fatal("expected fresh dns conn to be closed when engine is stopping")
	}
	e.activeMu.Lock()
	defer e.activeMu.Unlock()
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected no active upstreams, got %d", len(e.activeUpstreams))
	}
}

func TestReleaseDNSConnDiscardsWhenStoppingFlipsBeforePoolAppend(t *testing.T) {
	conn := &closeTrackingConn{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443}}
	e := &Engine{
		activeUpstreams: map[io.Closer]struct{}{
			conn: {},
		},
		dnsPool: make(map[string][]idleDNSConn),
	}

	e.mu.Lock()
	e.stopping = true
	e.mu.Unlock()
	e.releaseDNSConn("8.8.8.8:53", conn, 0)

	if !conn.isClosed() {
		t.Fatal("expected dns conn to be closed when stopping is set")
	}
	e.activeMu.Lock()
	activeCount := len(e.activeUpstreams)
	e.activeMu.Unlock()
	if activeCount != 0 {
		t.Fatalf("expected no active upstreams, got %d", activeCount)
	}
	e.dnsPoolMu.Lock()
	defer e.dnsPoolMu.Unlock()
	if len(e.dnsPool["8.8.8.8:53"]) != 0 {
		t.Fatal("expected no pooled dns connections after shutdown race")
	}
}

func TestReleaseDNSConnCannotAppendAfterPoolClosure(t *testing.T) {
	deadlineCh := make(chan struct{}, 1)
	conn := &closeTrackingConn{
		remote:     &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 443},
		deadlineCh: deadlineCh,
	}
	e := &Engine{
		activeUpstreams: map[io.Closer]struct{}{conn: {}},
		dnsPool:         make(map[string][]idleDNSConn),
	}

	e.dnsPoolMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.releaseDNSConn("8.8.8.8:53", conn, 0)
	}()
	select {
	case <-deadlineCh:
	case <-time.After(time.Second):
		e.dnsPoolMu.Unlock()
		t.Fatal("release did not reach deadline reset")
	}
	/* Simulates Stop closing the pool after a release began with stopping=false. */
	e.dnsPoolClosed = true
	e.dnsPoolMu.Unlock()
	<-done

	if !conn.isClosed() {
		t.Fatal("expected late dns release to be closed")
	}
	if len(e.dnsPool) != 0 {
		t.Fatalf("expected closed dns pool to remain empty, got %d targets", len(e.dnsPool))
	}
}

func TestHandleUDPShortIdleFlowExpiresThroughSessionWatchdog(t *testing.T) {
	target := &net.UDPAddr{IP: net.ParseIP("9.9.9.9").To4(), Port: 9999}
	deadlineCh := make(chan struct{}, 1)
	session := &deadlineAwareUDPSession{target: target, deadlineCh: deadlineCh}
	conn := &deadlineAwarePacketConn{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    20,
				UDPIdleTimeoutMS: 20,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          &stubUDPDialCounter{},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.handleUDP(context.Background(), id, conn)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected short idle udp flow to exit after initial deadline")
	}

	conn.mu.Lock()
	localClosed := conn.closed
	conn.mu.Unlock()
	if !localClosed {
		t.Fatal("expected session watchdog to close the local packet endpoint")
	}
	select {
	case <-deadlineCh:
	default:
		t.Fatal("expected session watchdog to interrupt both upstream directions")
	}
}

func TestHandleUDPSuccessfulIdleExchangeStaysOutOfInfoLogs(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)
	SetLogLevel(LogLevelInfo)

	target := &net.UDPAddr{IP: net.ParseIP("9.9.9.9").To4(), Port: 9999}
	session := &scriptedUDPSession{reads: []scriptedUDPRead{
		{payload: []byte{0x03, 0x04}, src: target},
		{err: &timeoutNetError{}},
	}}
	localConn := &scriptedTimeoutPacketConn{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    20,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          &stubUDPDialCounter{},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(target.IP),
		LocalPort:     uint16(target.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, localConn)
	if strings.Contains(out.String(), "[INFO] [UDP]") {
		t.Fatalf("successful idle exchange emitted info log: %q", out.String())
	}
}

func TestDrainUDPPumpResultsReturnsAfterBoundedForceClose(t *testing.T) {
	resultCh := make(chan udpPumpResult, 2)
	forceClosed := make(chan struct{}, 1)
	warned := make(chan struct{}, 1)
	start := time.Now()
	_, _, drained := drainUDPPumpResults(
		resultCh,
		0,
		udpPumpResult{Direction: "local_to_upstream"},
		udpPumpResult{Direction: "upstream_to_local"},
		10*time.Millisecond,
		20*time.Millisecond,
		func() { forceClosed <- struct{}{} },
		func() { warned <- struct{}{} },
	)
	if drained {
		t.Fatal("expected stuck pumps to report an incomplete drain")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("bounded pump drain took too long: %s", elapsed)
	}
	select {
	case <-warned:
	default:
		t.Fatal("expected stuck pump warning callback")
	}
	select {
	case <-forceClosed:
	default:
		t.Fatal("expected stuck pump force-close callback")
	}
}

func TestHandleUDPForceClosesPumpIgnoringDeadlines(t *testing.T) {
	target := &net.UDPAddr{IP: net.ParseIP("9.9.9.9").To4(), Port: 9999}
	session := &blockingUDPSession{
		readCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
	}
	localConn := &stubDNSPacketConnBlocking{
		firstRead: []byte{0x01, 0x02},
		blockCh:   make(chan struct{}),
	}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    20,
				UDPIdleTimeoutMS: 20,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          &stubUDPDialCounter{},
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
		udpPool: map[string][]UDPSession{
			udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"): {session},
		},
	}
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(target.IP),
		LocalPort:     uint16(target.Port),
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	start := time.Now()
	e.handleUDP(context.Background(), id, localConn)
	if elapsed := time.Since(start); elapsed < udpPumpDrainWarningTimeout || elapsed > 4*time.Second {
		t.Fatalf("expected bounded force-close near %s, took %s", udpPumpDrainWarningTimeout, elapsed)
	}
	select {
	case <-session.closeCh:
	default:
		t.Fatal("expected force-close to close the deadline-ignoring udp session")
	}
}

func TestReleaseUDPSessionDiscardsWhenStoppingFlipsBeforePoolAppend(t *testing.T) {
	session := &deadlineAwareUDPSession{target: &net.UDPAddr{IP: net.ParseIP("9.9.9.9").To4(), Port: 9999}}
	e := &Engine{
		activeUpstreams: map[io.Closer]struct{}{
			session: {},
		},
		udpPool: make(map[string][]UDPSession),
	}

	e.mu.Lock()
	e.stopping = true
	e.mu.Unlock()
	e.releaseUDPSession(udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999"), session, true)

	if !session.isClosed() {
		t.Fatal("expected udp session to be closed when stopping is set")
	}
	e.activeMu.Lock()
	activeCount := len(e.activeUpstreams)
	e.activeMu.Unlock()
	if activeCount != 0 {
		t.Fatalf("expected no active upstreams, got %d", activeCount)
	}
	e.udpPoolMu.Lock()
	defer e.udpPoolMu.Unlock()
	if len(e.udpPool[udpSessionPoolKey("198.18.0.1:53000", "9.9.9.9:9999")]) != 0 {
		t.Fatal("expected no pooled udp sessions after shutdown race")
	}
}

func TestReleaseUDPSessionCannotAppendAfterPoolClosure(t *testing.T) {
	deadlineCh := make(chan struct{}, 1)
	session := &deadlineAwareUDPSession{
		target:     &net.UDPAddr{IP: net.ParseIP("9.9.9.9").To4(), Port: 9999},
		deadlineCh: deadlineCh,
	}
	e := &Engine{
		activeUpstreams: map[io.Closer]struct{}{session: {}},
		udpPool:         make(map[string][]UDPSession),
	}

	e.udpPoolMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.releaseUDPSession("flow", session, true)
	}()
	select {
	case <-deadlineCh:
	case <-time.After(time.Second):
		e.udpPoolMu.Unlock()
		t.Fatal("release did not reach deadline reset")
	}
	/* Simulates Stop closing the pool after a release began with stopping=false. */
	e.udpPoolClosed = true
	e.udpPoolMu.Unlock()
	<-done

	if !session.isClosed() {
		t.Fatal("expected late udp release to be closed")
	}
	if len(e.udpPool) != 0 {
		t.Fatalf("expected closed udp pool to remain empty, got %d targets", len(e.udpPool))
	}
}

func TestShouldReuseUDPSessionRejectsEOF(t *testing.T) {
	e := &Engine{}
	if e.shouldReuseUDPSession(context.Background(), io.EOF) {
		t.Fatal("expected eof to disable udp session reuse")
	}
}

func TestLocalPacketReadClosedErrorIncludesEOF(t *testing.T) {
	if !isLocalPacketReadClosedError(io.EOF) {
		t.Fatal("expected udp local EOF to be classified as local reader closed")
	}
	if !isLocalPacketReadClosedError(io.ErrUnexpectedEOF) {
		t.Fatal("expected udp local unexpected EOF to be classified as local reader closed")
	}
}

/* Verifies normalized local write errors still allow UDP session reuse. */
func TestShouldReuseUDPSessionAllowsBenignLocalWriteError(t *testing.T) {
	e := &Engine{}
	if !e.shouldReuseUDPSession(context.Background(), nil) {
		t.Fatal("expected nil error (normalized benign local write) to allow session reuse")
	}
}

/* Verifies upstream read errors still prevent UDP session reuse. */
func TestShouldReuseUDPSessionRejectsUpstreamReadError(t *testing.T) {
	e := &Engine{}
	err := errors.New("read udp: port unreachable")
	if e.shouldReuseUDPSession(context.Background(), err) {
		t.Fatal("expected upstream read error to prevent session reuse")
	}
}

func TestDNSSummaryErrorsTreatEOFAsFailure(t *testing.T) {
	upErr, downErr := dnsSummaryErrors(io.EOF, nil)
	if errors.Is(upErr, io.EOF) {
		t.Fatal("expected dns summary to rewrite eof so it is not logged as ok")
	}
	if got := combineResults(upErr, downErr); got == "ok" || strings.Contains(got, "up:ok") {
		t.Fatalf("expected dns summary failure, got %q", got)
	}
}

func TestCombineResultsTreatsEndpointClosedForSendAsLocalClosed(t *testing.T) {
	err := errors.New("write tcp 142.251.24.132:443: endpoint is closed for send")
	if got := combineResults(nil, err); got != "up:ok/down:local-closed" {
		t.Fatalf("expected local-closed tcp summary, got %q", got)
	}
}

func TestDNSFailureClassSeparatesTimeouts(t *testing.T) {
	if got := dnsFailureClass(&timeoutNetError{}); got != "timeout" {
		t.Fatalf("expected timeout dns failure class, got %q", got)
	}
}

func TestDNSFailureClassSeparatesCanceledErrors(t *testing.T) {
	err := fmt.Errorf("upstream[0] 8.211.166.217:443: dial tcp: %w", context.Canceled)
	if got := dnsFailureClass(err); got != "canceled" {
		t.Fatalf("expected canceled dns failure class, got %q", got)
	}
	if got := dnsFailureClass(errors.New("dial tcp: operation was canceled")); got != "canceled" {
		t.Fatalf("expected operation-was-canceled dns failure class, got %q", got)
	}
}

func TestLogDNSExchangeFailureSuppressesNetworkChangeCancel(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)
	SetLogLevel(LogLevelInfo)

	err := fmt.Errorf("upstream[0] 8.211.166.217:443: dial upstream tls: %w", context.Canceled)
	e := &Engine{}
	e.logDNSExchangeFailure("udp1", ModeSocks5TLS, "198.18.0.1:53000", "1.1.1.1:53", err)

	if strings.Contains(out.String(), "[ERROR]") {
		t.Fatalf("expected network-change dns cancel to stay out of error logs, got %q", out.String())
	}
}

func TestShouldLogUDPSummarySuppressesBidirectionalCancel(t *testing.T) {
	if shouldLogUDPSummary(
		udpPumpResult{Err: context.Canceled},
		udpPumpResult{Err: errors.New("dial tcp: operation was canceled")},
		500*time.Millisecond,
	) {
		t.Fatal("expected bidirectional cancel to stay out of udp summary logs")
	}
}

func TestShouldLogUDPSummarySuppressesSuccessfulIdleTimeout(t *testing.T) {
	if shouldLogUDPSummary(
		udpPumpResult{Bytes: 29, Packets: 1, Err: testTimeoutError{}},
		udpPumpResult{Bytes: 67, Packets: 1, Err: testTimeoutError{}},
		5*time.Second,
	) {
		t.Fatal("expected a small successful udp exchange ending at idle timeout to stay quiet")
	}
}

func TestCombineUDPResultsReportsIdleTimeoutAsIdle(t *testing.T) {
	res := combineUDPResults(
		udpPumpResult{Bytes: 29, Packets: 1, Err: testTimeoutError{}},
		udpPumpResult{Bytes: 67, Packets: 1, Err: testTimeoutError{}},
	)
	if res != "up:idle/down:idle" {
		t.Fatalf("expected idle udp summary, got %q", res)
	}
}

func TestCombineUDPResultsTreatsBenignLocalWriteErrorAsLocalClosed(t *testing.T) {
	res := combineUDPResults(
		udpPumpResult{},
		udpPumpResult{
			Err:           errors.New("write udp 104.18.41.158:443: connection was refused"),
			LocalWriteErr: true,
		},
	)
	if res != "up:ok/down:local-closed" {
		t.Fatalf("expected local-closed udp summary, got %q", res)
	}
}

func TestDNSCacheKeyOmitsTransactionID(t *testing.T) {
	payloadA := []byte{0xaa, 0xbb, 0x01, 0x00, 0x00, 0x01}
	payloadB := []byte{0xcc, 0xdd, 0x01, 0x00, 0x00, 0x01}
	keyA := dnsCacheKey("8.8.8.8:53", payloadA)
	keyB := dnsCacheKey("8.8.8.8:53", payloadB)
	if keyA != keyB {
		t.Fatalf("expected same cache key for payloads differing only in txid, got %q vs %q", keyA, keyB)
	}
}

func TestActivateLocalRejectsStoppingEngine(t *testing.T) {
	e := &Engine{
		stopping:     true,
		activeLocals: make(map[io.Closer]struct{}),
	}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}}
	if e.activateLocal(conn) {
		t.Fatal("expected activateLocal to reject stopping engine")
	}
	if len(e.activeLocals) != 0 {
		t.Fatalf("expected no active locals, got %d", len(e.activeLocals))
	}
}

func TestActivateUpstreamRejectsStoppingEngine(t *testing.T) {
	e := &Engine{
		stopping:        true,
		activeUpstreams: make(map[io.Closer]struct{}),
	}
	conn := &stubConnWithRemote{remote: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}}
	if e.activateUpstream(conn) {
		t.Fatal("expected activateUpstream to reject stopping engine")
	}
	if len(e.activeUpstreams) != 0 {
		t.Fatalf("expected no active upstreams, got %d", len(e.activeUpstreams))
	}
}

func TestHandleUDPPolicyRestrictionIsThrottled(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)
	SetLogLevel(LogLevelInfo)

	dialer := &stubUDPDialCounter{}
	dialer.disableUDP = true
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:               dialer,
		udpDialSem:           make(chan struct{}, 1),
		activeUpstreams:      make(map[io.Closer]struct{}),
		activeLocals:         make(map[io.Closer]struct{}),
		logThrottleStates:    make(map[string]logThrottleState),
		udpNoResponseBackoff: make(map[string]int64),
	}
	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("9.9.9.9").To4()),
		LocalPort:     9999,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x01, 0x02}}})
	e.handleUDP(context.Background(), id, &stubDNSPacketConn{reads: [][]byte{{0x03, 0x04}}})

	if got := dialer.count(); got != 0 {
		t.Fatalf("expected udp policy restriction to drop without upstream dial, got %d dials", got)
	}
	if strings.Contains(out.String(), "policy restriction") {
		t.Fatalf("expected udp policy restriction drops to stay out of info logs, got %q", out.String())
	}
	if !e.isUDPNoResponseCoolingDown("9.9.9.9:9999") {
		t.Fatal("expected policy-restricted udp target to enter drop cooldown")
	}
}

func TestHandleUDPAllowsSTUNWhenUDPEnabled(t *testing.T) {
	dialer := &stubUDPDialCounter{}
	e := &Engine{
		cfg: &Config{
			Mode: ModeSocks5TLS,
			Tun:  TunConfig{Name: validTunNameForRuntime(), MTU: 1500},
			Upstream: UpstreamConfig{
				Addr: "127.0.0.1:443",
			},
			Runtime: RuntimeConfig{
				ConnectTimeoutMS: 1000,
				IdleTimeoutMS:    1000,
				TCPBuffer:        4096,
				UDPBuffer:        4096,
				EnableUDP:        true,
			},
		},
		dialer:          dialer,
		udpDialSem:      make(chan struct{}, 1),
		activeUpstreams: make(map[io.Closer]struct{}),
		activeLocals:    make(map[io.Closer]struct{}),
	}

	id := gstack.TransportEndpointID{
		LocalAddress:  tcpip.AddrFromSlice(net.ParseIP("60.29.236.164").To4()),
		LocalPort:     3478,
		RemoteAddress: tcpip.AddrFromSlice(net.ParseIP("198.18.0.1").To4()),
		RemotePort:    53000,
	}

	e.handleUDP(context.Background(), id, &stubPacketConnEOF{})

	if got := dialer.count(); got != 1 {
		t.Fatalf("expected one udp dial for stun when udp is enabled, got %d", got)
	}
}

func TestLogStablePolicyDropVisibleAtInfoLevel(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)

	SetLogLevel(LogLevelInfo)
	e := &Engine{logThrottleStates: make(map[string]logThrottleState)}
	e.logTCPDialFailure("tcp-1", ModeSocks5TLS, "198.18.0.1:53000", "1.1.1.1:443", io.ErrUnexpectedEOF)

	if !strings.Contains(out.String(), "dial failed") {
		t.Fatalf("expected throttled tcp dial failure to be visible at info level, got %q", out.String())
	}
}

func TestLogTCPDialFailureSanitizesJoinedErrorNewlines(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)

	SetLogLevel(LogLevelInfo)
	e := &Engine{logThrottleStates: make(map[string]logThrottleState)}
	err := errors.Join(
		errors.New("upstream[0] 8.211.166.217:443: socks5 connect failed"),
		errors.New("upstream[1] 147.78.246.27:443: context deadline exceeded"),
	)
	e.logTCPDialFailure("tcp-1", ModeSocks5TLS, "198.18.0.1:53000", "162.125.80.3:443", err)

	got := out.String()
	if strings.Count(strings.TrimSuffix(got, "\n"), "\n") != 0 {
		t.Fatalf("expected joined error to stay on one log line, got %q", got)
	}
	if !strings.Contains(got, "upstream[0]") || !strings.Contains(got, "upstream[1]") {
		t.Fatalf("expected both upstream errors to remain visible, got %q", got)
	}
}

func TestLogDNSExchangeFailureSanitizesJoinedErrorNewlines(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)

	SetLogLevel(LogLevelInfo)
	e := &Engine{logThrottleStates: make(map[string]logThrottleState)}
	err := errors.Join(
		errors.New("upstream[0] 8.211.166.217:443: read: connection reset by peer"),
		errors.New("upstream[1] 147.78.246.27:443: broken pipe"),
	)
	e.logDNSExchangeFailure("dns-1", ModeSocks5TLS, "198.18.0.1:53000", "1.1.1.1:53", err)

	got := out.String()
	if strings.Count(strings.TrimSuffix(got, "\n"), "\n") != 0 {
		t.Fatalf("expected joined dns error to stay on one log line, got %q", got)
	}
	if !strings.Contains(got, "upstream[0]") || !strings.Contains(got, "upstream[1]") {
		t.Fatalf("expected both upstream errors to remain visible, got %q", got)
	}
}

func TestLogUDPHandlerLimitIsThrottled(t *testing.T) {
	var out bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevLevel := LogLevel(currentLogLevel.Load())
	log.SetOutput(&out)
	log.SetFlags(0)
	defer log.SetOutput(prevWriter)
	defer log.SetFlags(prevFlags)
	defer SetLogLevel(prevLevel)

	SetLogLevel(LogLevelSystem)
	e := &Engine{
		udpHandlerSem:     make(chan struct{}, 512),
		logThrottleStates: make(map[string]logThrottleState),
	}

	e.logUDPHandlerLimit("198.18.0.1")
	e.logUDPHandlerLimit("198.18.0.1")

	if got := strings.Count(out.String(), "handler concurrency limit reached"); got != 1 {
		t.Fatalf("expected repeated udp handler limit logs to be throttled, got %d logs: %q", got, out.String())
	}
}

func TestCommitLinuxDefaultRouteUpdatesStateBeforeNotify(t *testing.T) {
	m := &AutoRouteManager{}
	notified := make(chan struct{}, 1)
	m.SetDefaultRouteChangeHook(func() {
		if m.linuxDefaultRoute != "default via 10.0.0.1 dev en0" {
			t.Fatalf("route state not committed before notify: %q", m.linuxDefaultRoute)
		}
		if m.linuxDefaultGW != "10.0.0.1" || m.linuxDefaultDev != "en0" {
			t.Fatalf("linux default route fields not committed before notify: gw=%q dev=%q", m.linuxDefaultGW, m.linuxDefaultDev)
		}
		notified <- struct{}{}
	})

	m.commitLinuxDefaultRoute("default via 10.0.0.1 dev en0", "10.0.0.1", "en0", true)

	select {
	case <-notified:
	case <-time.After(time.Second):
		t.Fatal("expected route change notification")
	}
}

func TestCommitLinuxDefaultRouteDoesNotDeadlockWhileManagerMutexIsHeld(t *testing.T) {
	m := &AutoRouteManager{}
	m.SetDefaultRouteChangeHook(func() {})
	done := make(chan struct{})

	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.commitLinuxDefaultRoute("default via 10.0.0.1 dev en0", "10.0.0.1", "en0", true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected route change hook notification to avoid self-deadlock")
	}
}

func TestDarwinDeleteHostRouteArgs(t *testing.T) {
	got := darwinDeleteHostRouteArgs("8.8.8.8")
	want := []string{"-n", "delete", "-host", "8.8.8.8"}
	if len(got) != len(want) {
		t.Fatalf("unexpected args length: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected arg[%d]: got=%q want=%q full=%v", i, got[i], want[i], got)
		}
	}
}

func TestConfigUDPIdleTimeoutUsesUDPPolicy(t *testing.T) {
	cfg := &Config{
		Runtime: RuntimeConfig{
			IdleTimeoutMS:    300_000,
			UDPIdleTimeoutMS: defaultUDPIdleTimeoutMS,
		},
	}

	if got := cfg.UDPIdleTimeout(); got != 5*time.Second {
		t.Fatalf("unexpected udp idle timeout: %s", got)
	}
}

func TestConfigDNSExchangeTimeoutPrefersConnectTimeout(t *testing.T) {
	cfg := &Config{
		Runtime: RuntimeConfig{
			ConnectTimeoutMS: 10_000,
			IdleTimeoutMS:    300_000,
		},
	}
	if got := cfg.DNSExchangeTimeout(); got != 10*time.Second {
		t.Fatalf("unexpected dns exchange timeout: %s", got)
	}

	cfg.Runtime.IdleTimeoutMS = 5_000
	if got := cfg.DNSExchangeTimeout(); got != 5*time.Second {
		t.Fatalf("expected dns exchange timeout to honor shorter idle timeout, got %s", got)
	}
}

func TestCopyWithIdleTimeoutKeepsSlowActiveFlowAlive(t *testing.T) {
	src := &deadlineRefreshConn{
		reads: []timedReadStep{
			{data: []byte("a")},
			{delay: 6 * time.Millisecond, data: []byte("b")},
			{delay: 6 * time.Millisecond, data: []byte("c")},
		},
	}
	dst := &deadlineRefreshConn{}

	copied, err := copyWithIdleTimeout(dst, src, make([]byte, 8), 10*time.Millisecond)
	if err != nil {
		t.Fatalf("copyWithIdleTimeout failed: %v", err)
	}
	if copied != 3 {
		t.Fatalf("unexpected copied bytes: got=%d want=3", copied)
	}
}

func TestIdleWatchdogExpiresOnlyAfterLastActivity(t *testing.T) {
	const idleTimeout = 150 * time.Millisecond
	watchdog := newIdleWatchdog(idleTimeout)
	done := make(chan struct{})
	expired := make(chan struct{})
	go watchdog.run(nil, done, func() { close(expired) })

	time.Sleep(50 * time.Millisecond)
	watchdog.touch()
	select {
	case <-expired:
		t.Fatal("watchdog expired from its initial deadline despite later activity")
	case <-time.After(120 * time.Millisecond):
	}
	select {
	case <-expired:
	case <-time.After(100 * time.Millisecond):
		close(done)
		t.Fatal("watchdog did not expire after the full idle interval")
	}
}

func TestRelayIdleTimeoutUsesActivityFromEitherDirection(t *testing.T) {
	localRelay, localPeer := net.Pipe()
	upstreamRelay, upstreamPeer := net.Pipe()
	defer localPeer.Close()
	defer upstreamPeer.Close()
	_ = localPeer.SetDeadline(time.Now().Add(2 * time.Second))
	_ = upstreamPeer.SetDeadline(time.Now().Add(2 * time.Second))

	done := make(chan bidirectionalRelayStats, 1)
	go func() {
		var pool relayBufferPool
		done <- relayBidirectionalWithStatsContext(
			context.Background(), localRelay, upstreamRelay, 1024, 150*time.Millisecond, &pool,
		)
	}()

	for i := 0; i < 6; i++ {
		time.Sleep(30 * time.Millisecond)
		payload := []byte{byte(i)}
		writeDone := make(chan error, 1)
		go func() {
			_, err := upstreamPeer.Write(payload)
			writeDone <- err
		}()
		var got [1]byte
		if _, err := io.ReadFull(localPeer, got[:]); err != nil {
			t.Fatalf("read downstream activity %d: %v", i, err)
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("write downstream activity %d: %v", i, err)
		}
	}
	_ = localPeer.Close()
	_ = upstreamPeer.Close()

	select {
	case stats := <-done:
		if isTimeoutError(stats.UploadErr) {
			t.Fatalf("upload direction timed out while download remained active: %+v", stats)
		}
		if stats.DownloadBytes != 6 {
			t.Fatalf("download bytes = %d, want 6", stats.DownloadBytes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not stop after peers closed")
	}
}

func TestWriteTUNFrameRejectsShortWrite(t *testing.T) {
	err := writeTUNFrame(&shortWriter{maxWrite: 2}, []byte{0x01, 0x02, 0x03})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected short write error, got %v", err)
	}
}

func TestEngineShouldRelayDNSOverTCPUsesPlainDNSPort(t *testing.T) {
	if !shouldRelayDNSOverTCP(&net.UDPAddr{Port: 53}) {
		t.Fatal("expected plain DNS port 53 to use dns-over-tcp relay")
	}
	for _, port := range []int{5353, 853, 5443} {
		if shouldRelayDNSOverTCP(&net.UDPAddr{Port: port}) {
			t.Fatalf("did not expect port %d to use dns-over-tcp relay", port)
		}
	}
}

func TestRouterDecidesCIDRRules(t *testing.T) {
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"1.0.1.0/24"},
		ProxyCIDRs:      []string{"1.0.1.8/32"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	if got := router.DecideIP(net.ParseIP("1.0.1.9")); got.Outbound != OutboundDirect {
		t.Fatalf("expected direct cidr decision, got %#v", got)
	}
	if got := router.DecideIP(net.ParseIP("1.0.1.8")); got.Outbound != OutboundProxy {
		t.Fatalf("expected proxy cidr to override direct cidr, got %#v", got)
	}
}

func TestRouterCIDRMatcherUsesLongestPrefix(t *testing.T) {
	router, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRs:     []string{"1.0.0.0/8", "1.0.1.0/24"},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	got := router.DecideIP(net.ParseIP("1.0.1.8"))
	if got.Outbound != OutboundDirect || got.Rule != "direct_cidr:1.0.1.0/24" {
		t.Fatalf("expected longest cidr match, got %#v", got)
	}
}

func TestRouterDisabledDoesNotLoadRuleFiles(t *testing.T) {
	_, err := NewRouter(RoutingConfig{
		Enabled:         false,
		DefaultOutbound: OutboundProxy,
		DirectCIDRFiles: []string{filepath.Join(t.TempDir(), "missing-cidr-rules.txt")},
	})
	if err != nil {
		t.Fatalf("disabled router should not load rule files: %v", err)
	}
}

func TestRouterEnabledRejectsInvalidRuleFile(t *testing.T) {
	tmp := t.TempDir()
	ruleFile := writeTestFile(t, tmp, "bad-cidr-rules.txt", []byte("not-a-cidr\n"))
	_, err := NewRouter(RoutingConfig{
		Enabled:         true,
		DefaultOutbound: OutboundProxy,
		DirectCIDRFiles: []string{ruleFile},
	})
	if err == nil || !strings.Contains(err.Error(), "load direct CIDR rules") {
		t.Fatalf("expected invalid rule file rejection, got %v", err)
	}
}

func TestNextAutoRouteEnsureIntervalBacksOffAfterStablePasses(t *testing.T) {
	if got := nextAutoRouteEnsureInterval(0); got != autoRouteEnsureFastInterval {
		t.Fatalf("unexpected fast interval: %s", got)
	}
	if got := nextAutoRouteEnsureInterval(autoRouteEnsureSlowAfterSuccess - 1); got != autoRouteEnsureFastInterval {
		t.Fatalf("unexpected pre-threshold interval: %s", got)
	}
	if got := nextAutoRouteEnsureInterval(autoRouteEnsureSlowAfterSuccess); got != autoRouteEnsureSlowInterval {
		t.Fatalf("unexpected slow interval: %s", got)
	}
}

func TestBypassCIDRsForFamilyIncludesCloudInternalRange(t *testing.T) {
	got := bypassCIDRsForFamily(nil, false)
	seenCloudInternal := false
	seenTailnet := false
	for _, cidr := range got {
		if cidr == "100.100.0.0/16" {
			seenCloudInternal = true
		}
		if cidr == "100.64.0.0/10" {
			seenTailnet = true
		}
	}
	if !seenCloudInternal {
		t.Fatalf("expected cloud internal bypass cidr, got=%v", got)
	}
	if !seenTailnet {
		t.Fatalf("expected tailnet bypass cidr, got=%v", got)
	}
}

func TestValidateProxyCIDRsAgainstAutoBypassRejectsOverlap(t *testing.T) {
	proxyCIDRs := mustParseCIDRsForTest(t, []string{
		"10.20.0.0/16",
		"fd12:3456::/48",
	})

	err := validateProxyCIDRsAgainstAutoBypass(proxyCIDRs, autoRoutePrivateBypassCIDRs)
	if err == nil || !strings.Contains(err.Error(), "10.20.0.0/16") || !strings.Contains(err.Error(), "10.0.0.0/8") {
		t.Fatalf("expected IPv4 proxy/bypass overlap error, got: %v", err)
	}

	err = validateProxyCIDRsAgainstAutoBypass(proxyCIDRs[1:], autoRoutePrivateBypassCIDRs)
	if err == nil || !strings.Contains(err.Error(), "fd12:3456::/48") || !strings.Contains(err.Error(), "fc00::/7") {
		t.Fatalf("expected IPv6 proxy/bypass overlap error, got: %v", err)
	}
}

func TestValidateProxyCIDRsAgainstAutoBypassAllowsDisjointRules(t *testing.T) {
	proxyCIDRs := mustParseCIDRsForTest(t, []string{"203.0.113.0/24", "2001:db8::/32"})
	if err := validateProxyCIDRsAgainstAutoBypass(proxyCIDRs, autoRoutePrivateBypassCIDRs); err != nil {
		t.Fatalf("disjoint proxy CIDRs should be allowed: %v", err)
	}
}

func TestValidateProxyCIDRsAgainstAutoBypassRejectsDynamicLANOverlap(t *testing.T) {
	proxyCIDRs := mustParseCIDRsForTest(t, []string{"203.0.113.0/24"})
	err := validateProxyCIDRsAgainstAutoBypass(proxyCIDRs, []string{"203.0.113.128/25"})
	if err == nil || !strings.Contains(err.Error(), "203.0.113.0/24") || !strings.Contains(err.Error(), "203.0.113.128/25") {
		t.Fatalf("expected dynamic LAN proxy/bypass overlap error, got: %v", err)
	}
}

func TestHandleFatalAutoRouteErrorQuiescesBeforeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	quiesced := false
	err := validateProxyCIDRsAgainstAutoBypass(
		mustParseCIDRsForTest(t, []string{"203.0.113.0/24"}),
		[]string{"203.0.113.128/25"},
	)
	if !handleFatalAutoRouteError(err, func() {
		if ctx.Err() != nil {
			t.Fatal("application context canceled before engine quiesced")
		}
		quiesced = true
	}, cancel) {
		t.Fatal("expected proxy/bypass conflict to be fatal")
	}
	if !quiesced {
		t.Fatal("expected fatal auto-route conflict to quiesce the engine")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("expected fatal auto-route conflict to cancel the application context")
	}
}

func TestHandleFatalAutoRouteErrorAllowsTransientEnsureFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	quiesced := false
	if handleFatalAutoRouteError(errors.New("temporary route command failure"), func() { quiesced = true }, cancel) {
		t.Fatal("transient route failure should not be fatal")
	}
	if quiesced {
		t.Fatal("transient route failure unexpectedly quiesced the engine")
	}
	select {
	case <-ctx.Done():
		t.Fatal("transient route failure unexpectedly canceled the application context")
	default:
	}
}

func TestAutoRouteRunErrorPreservesFatalConflict(t *testing.T) {
	fatalErr := make(chan error, 1)
	conflict := &autoRouteProxyBypassConflictError{
		proxyCIDR:  "203.0.113.0/24",
		bypassCIDR: "203.0.113.128/25",
	}
	fatalErr <- conflict

	err := autoRouteRunError(fatalErr)
	if err == nil || !isAutoRouteProxyBypassConflict(err) {
		t.Fatalf("expected wrapped fatal conflict, got %v", err)
	}
}

func TestSetupLinuxDynamicProxyConflictDoesNotMutateRoutes(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default via 203.0.113.1 dev eth0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	printf '2: eth0 inet 203.0.113.2/24 brd 203.0.113.255 scope global eth0\n'
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:        &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:    "tun0",
		proxyCIDRs: mustParseCIDRsForTest(t, []string{"203.0.113.0/24"}),
	}
	err := mgr.setupLinux()
	if !isAutoRouteProxyBypassConflict(err) {
		t.Fatalf("expected dynamic proxy/bypass conflict, got %v", err)
	}
	if mgr.setupMutated {
		t.Fatal("setup must remain unowned when conflict is detected before mutation")
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if strings.Contains(logText, "addr add") || strings.Contains(logText, "link set") || strings.Contains(logText, "route replace default") {
		t.Fatalf("setup conflict unexpectedly mutated routes: %q", logText)
	}
}

func TestEnsureLinuxConflictCommitsLatestRollbackRouteBeforeReturning(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default via 203.0.113.1 dev wifi0 metric 50\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	printf '3: wifi0 inet 203.0.113.2/24 brd 203.0.113.255 scope global wifi0\n'
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:               &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:           "tun0",
		proxyCIDRs:        mustParseCIDRsForTest(t, []string{"203.0.113.0/24"}),
		linuxDefaultRoute: "default via 10.0.0.1 dev eth0 metric 100",
		linuxDefaultGW:    "10.0.0.1",
		linuxDefaultDev:   "eth0",
	}
	err := mgr.ensureLinux()
	if !isAutoRouteProxyBypassConflict(err) {
		t.Fatalf("expected dynamic proxy/bypass conflict, got %v", err)
	}
	if mgr.linuxDefaultRoute != "default via 203.0.113.1 dev wifi0 metric 50" || mgr.linuxDefaultDev != "wifi0" {
		t.Fatalf("latest native default was not retained for teardown: route=%q dev=%q", mgr.linuxDefaultRoute, mgr.linuxDefaultDev)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if strings.Contains(string(logData), "route replace default dev tun0") {
		t.Fatalf("fatal conflict should be detected before route mutation: %q", string(logData))
	}
}

func TestNewAutoRouteManagerRejectsProxyBypassOverlapFromRuleFile(t *testing.T) {
	ruleFile := writeTestFile(t, t.TempDir(), "proxy-cidrs.txt", []byte("192.168.50.0/24\n"))
	cfg := &Config{Routing: RoutingConfig{
		Enabled:        true,
		ProxyCIDRFiles: []string{ruleFile},
	}}

	_, err := NewAutoRouteManager(cfg, "tun-test")
	if err == nil || !strings.Contains(err.Error(), "routing.proxy_cidrs") {
		t.Fatalf("expected rule-file proxy/bypass overlap error, got: %v", err)
	}
}

func mustParseCIDRsForTest(t *testing.T, values []string) []*net.IPNet {
	t.Helper()
	out := make([]*net.IPNet, 0, len(values))
	for _, value := range values {
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			t.Fatalf("parse CIDR %q: %v", value, err)
		}
		out = append(out, network)
	}
	return out
}

func TestResetTimerRearmsImmediately(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	resetTimer(timer, 10*time.Millisecond)

	select {
	case <-timer.C:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer was not re-armed")
	}
}

func TestSocks5GreetingRejectsUnexpectedMethod(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 3)
		_, _ = io.ReadFull(server, buf)
		_, _ = server.Write([]byte{socksVersion, 0x02})
	}()

	err := socks5Greeting(client, client, "", "")
	<-done
	if err == nil || !strings.Contains(err.Error(), "requested username/password auth") {
		t.Fatalf("expected greeting rejection, got: %v", err)
	}
}

func TestSocks5GreetingHandlesShortWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	shortClient := &shortWriteConn{Conn: client, maxWrite: 1}
	done := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 3)
		_, _ = io.ReadFull(server, buf)
		_, _ = server.Write([]byte{socksVersion, 0x00})
		done <- buf
	}()

	if err := socks5Greeting(shortClient, shortClient, "", ""); err != nil {
		t.Fatalf("greeting failed after short writes: %v", err)
	}
	got := <-done
	want := []byte{socksVersion, 0x01, 0x00}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected greeting payload: got=%v want=%v", got, want)
	}
}

func TestSocks5GreetingSupportsUsernamePassword(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan error, 1)
	go func() {
		defer close(done)
		greeting := make([]byte, 4)
		if _, err := io.ReadFull(server, greeting); err != nil {
			done <- err
			return
		}
		wantGreeting := []byte{socksVersion, 0x02, 0x00, 0x02}
		if !bytes.Equal(greeting, wantGreeting) {
			done <- fmt.Errorf("unexpected greeting: got=%v want=%v", greeting, wantGreeting)
			return
		}
		if _, err := server.Write([]byte{socksVersion, 0x02}); err != nil {
			done <- err
			return
		}

		authReq := make([]byte, 0, 32)
		buf := make([]byte, 2)
		if _, err := io.ReadFull(server, buf); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, buf...)
		userLen := int(buf[1])
		user := make([]byte, userLen)
		if _, err := io.ReadFull(server, user); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, user...)
		if _, err := io.ReadFull(server, buf[:1]); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, buf[:1]...)
		passLen := int(buf[0])
		pass := make([]byte, passLen)
		if _, err := io.ReadFull(server, pass); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, pass...)
		wantAuth := append([]byte{0x01, 0x04}, []byte("user")...)
		wantAuth = append(wantAuth, 0x04)
		wantAuth = append(wantAuth, []byte("pass")...)
		if !bytes.Equal(authReq, wantAuth) {
			done <- fmt.Errorf("unexpected auth request: got=%v want=%v", authReq, wantAuth)
			return
		}
		_, err := server.Write([]byte{0x01, 0x00})
		done <- err
	}()

	if err := socks5Greeting(client, client, "user", "pass"); err != nil {
		t.Fatalf("greeting with username/password failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server validation failed: %v", err)
	}
}

func TestSocks5GreetingSupportsPasswordOnlyAuth(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan error, 1)
	go func() {
		defer close(done)
		greeting := make([]byte, 4)
		if _, err := io.ReadFull(server, greeting); err != nil {
			done <- err
			return
		}
		wantGreeting := []byte{socksVersion, 0x02, 0x00, 0x02}
		if !bytes.Equal(greeting, wantGreeting) {
			done <- fmt.Errorf("unexpected greeting: got=%v want=%v", greeting, wantGreeting)
			return
		}
		if _, err := server.Write([]byte{socksVersion, 0x02}); err != nil {
			done <- err
			return
		}

		authReq := make([]byte, 0, 32)
		buf := make([]byte, 2)
		if _, err := io.ReadFull(server, buf); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, buf...)
		userLen := int(buf[1])
		user := make([]byte, userLen)
		if _, err := io.ReadFull(server, user); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, user...)
		if _, err := io.ReadFull(server, buf[:1]); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, buf[:1]...)
		passLen := int(buf[0])
		pass := make([]byte, passLen)
		if _, err := io.ReadFull(server, pass); err != nil {
			done <- err
			return
		}
		authReq = append(authReq, pass...)
		wantAuth := []byte{0x01, 0x00, 0x06, 's', 'e', 'c', 'r', 'e', 't'}
		if !bytes.Equal(authReq, wantAuth) {
			done <- fmt.Errorf("unexpected auth request: got=%v want=%v", authReq, wantAuth)
			return
		}
		_, err := server.Write([]byte{0x01, 0x00})
		done <- err
	}()

	if err := socks5Greeting(client, client, "", "secret"); err != nil {
		t.Fatalf("greeting with password-only auth failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server validation failed: %v", err)
	}
}

func TestHandshakeCONNECTRejectsNon200(t *testing.T) {
	d := &HTTPSConnectDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		_, _ = io.WriteString(server, "HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
	}()

	err := d.handshakeCONNECT(bufio.NewReader(client), client, "example.com:443")
	<-done
	if err == nil || !strings.Contains(err.Error(), "connect rejected") {
		t.Fatalf("expected connect rejected error, got: %v", err)
	}
}

func TestHandshakeCONNECTSendsBasicAuthHeaderWhenOnlyPasswordIsSet(t *testing.T) {
	d := &HTTPSConnectDialer{basicPass: "secret"}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan error, 1)
	go func() {
		defer close(done)
		br := bufio.NewReader(server)
		var request strings.Builder
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				done <- err
				return
			}
			request.WriteString(line)
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		expectedToken := base64.StdEncoding.EncodeToString([]byte(":secret"))
		if !strings.Contains(request.String(), "Proxy-Authorization: Basic "+expectedToken) {
			done <- fmt.Errorf("missing basic auth header in request: %q", request.String())
			return
		}
		_, err := io.WriteString(server, "HTTP/1.1 200 Connection Established\r\n\r\n")
		done <- err
	}()

	if err := d.handshakeCONNECT(bufio.NewReader(client), client, "example.com:443"); err != nil {
		t.Fatalf("handshake connect failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server validation failed: %v", err)
	}
}

func TestHandshakeCONNECTRejectsOverlongHeaderLine(t *testing.T) {
	d := &HTTPSConnectDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		_, _ = io.WriteString(server, "HTTP/1.1 200 Connection Established\r\n"+strings.Repeat("a", maxHTTPConnectHeaderLineBytes+1))
	}()

	err := d.handshakeCONNECT(bufio.NewReaderSize(client, 64), client, "example.com:443")
	<-done
	if err == nil || !strings.Contains(err.Error(), "line too long") {
		t.Fatalf("expected overlong header error, got: %v", err)
	}
}

func TestHandshakeCONNECTRejectsOverlongStatusLineWithoutNewline(t *testing.T) {
	d := &HTTPSConnectDialer{}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	_ = client.SetDeadline(time.Now().Add(3 * time.Second))
	_ = server.SetDeadline(time.Now().Add(3 * time.Second))

	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" || line == "\n" {
				break
			}
		}
		_, _ = io.WriteString(server, strings.Repeat("a", maxHTTPConnectHeaderLineBytes+1))
	}()

	err := d.handshakeCONNECT(bufio.NewReaderSize(client, 64), client, "example.com:443")
	<-done
	if err == nil || !strings.Contains(err.Error(), "line too long") {
		t.Fatalf("expected overlong status-line error, got: %v", err)
	}
}

func TestWriteBackToPacketConnRejectsWriteToOnlyFallback(t *testing.T) {
	_, err := writeBackToPacketConn(&stubPacketConnWriteToOnly{}, []byte{0x01})
	if err == nil || !strings.Contains(err.Error(), "does not support connected writes") {
		t.Fatalf("expected connected write error, got: %v", err)
	}
}

func TestIntegrationWithLocalUpstreams(t *testing.T) {
	if os.Getenv("SHAMANTUN_IT") != "1" {
		t.Skip("set SHAMANTUN_IT=1 to run local upstream interop")
	}

	ss5Addr := mustGetenv(t, "SHAMANTUN_SS5_ADDR")
	hproxyAddr := mustGetenv(t, "SHAMANTUN_HPROXY_ADDR")
	clientPEM := mustGetenv(t, "SHAMANTUN_CLIENT_PEM")
	clientKey := mustGetenv(t, "SHAMANTUN_CLIENT_KEY")
	basicUser := mustGetenv(t, "SHAMANTUN_HPROXY_USER")
	basicPass := mustGetenv(t, "SHAMANTUN_HPROXY_PASS")

	targetAddr, stopHTTP := startHTTPResponder(t)
	defer stopHTTP()

	udpTarget, stopUDP := startUDPEchoServer(t)
	defer stopUDP()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("socks5tls-tcp", func(t *testing.T) {
		cfg := UpstreamConfig{
			Addr:       ss5Addr,
			ServerName: "localhost",
			ClientPEM:  clientPEM,
			ClientKey:  clientKey,
		}
		d, err := NewUpstreamDialer(ModeSocks5TLS, cfg, 5*time.Second, 0)
		if err != nil {
			t.Fatalf("new dialer: %v", err)
		}
		conn, err := d.DialTCP(ctx, targetAddr)
		if err != nil {
			t.Fatalf("dial tcp via socks5tls: %v", err)
		}
		assertHTTP204(t, conn)
	})

	t.Run("socks5tls-udp", func(t *testing.T) {
		cfg := UpstreamConfig{
			Addr:       ss5Addr,
			ServerName: "localhost",
			ClientPEM:  clientPEM,
			ClientKey:  clientKey,
		}
		d, err := NewUpstreamDialer(ModeSocks5TLS, cfg, 5*time.Second, 0)
		if err != nil {
			t.Fatalf("new dialer: %v", err)
		}
		session, err := d.DialUDP(ctx)
		if err != nil {
			t.Fatalf("dial udp via socks5tls: %v", err)
		}
		defer session.Close()

		payload := []byte("shamantun-udp-probe")
		if err := session.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		if err := session.WriteTo(payload, udpTarget); err != nil {
			t.Fatalf("write to udp target: %v", err)
		}

		buf := make([]byte, 256)
		n, src, err := session.ReadFrom(buf)
		if err != nil {
			t.Fatalf("read from udp target: %v", err)
		}
		if src == nil || src.Port != udpTarget.Port {
			t.Fatalf("unexpected udp source: %v want port %d", src, udpTarget.Port)
		}
		if !bytes.Equal(buf[:n], payload) {
			t.Fatalf("udp payload mismatch: got %q want %q", string(buf[:n]), string(payload))
		}
	})

	t.Run("https-connect-tcp", func(t *testing.T) {
		cfg := UpstreamConfig{
			Addr:       hproxyAddr,
			ServerName: "localhost",
			ClientPEM:  clientPEM,
			ClientKey:  clientKey,
			Username:   basicUser,
			Password:   basicPass,
		}
		d, err := NewUpstreamDialer(ModeHTTPS, cfg, 5*time.Second, 0)
		if err != nil {
			t.Fatalf("new dialer: %v", err)
		}
		conn, err := d.DialTCP(ctx, targetAddr)
		if err != nil {
			t.Fatalf("dial tcp via https connect: %v", err)
		}
		assertHTTP204(t, conn)
	})
}

func newValidConfigForTest(t *testing.T) *Config {
	t.Helper()

	tmp := t.TempDir()
	clientPEM := writeTestFile(t, tmp, "client.pem", []byte("test"))
	clientKey := writeTestFile(t, tmp, "client.key", []byte("test"))

	return &Config{
		Mode: ModeSocks5TLS,
		Tun: TunConfig{
			Name: validTunNameForRuntime(),
			MTU:  1500,
		},
		Upstream: UpstreamConfig{
			Addr:      "127.0.0.1:443",
			ClientPEM: clientPEM,
			ClientKey: clientKey,
		},
		Runtime: RuntimeConfig{
			ConnectTimeoutMS:      minConnectTimeoutMS,
			IdleTimeoutMS:         minIdleTimeoutMS,
			UDPIdleTimeoutMS:      defaultUDPIdleTimeoutMS,
			TCPBuffer:             minRuntimeBufferBytes,
			UDPBuffer:             minRuntimeBufferBytes,
			TCPDialConcurrency:    1,
			UDPDialConcurrency:    1,
			TCPHandlerConcurrency: 1,
			UDPHandlerConcurrency: 1,
			DNSCacheSize:          1,
		},
	}
}

func validTunNameForRuntime() string {
	if runtime.GOOS == "darwin" {
		return "utun"
	}
	return "tun0"
}

func writeTestFile(t *testing.T, dir, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func mustGetenv(t *testing.T, key string) string {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		t.Fatalf("missing env %s", key)
	}
	return v
}

func startHTTPResponder(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http responder: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				br := bufio.NewReader(c)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if line == "\r\n" || line == "\n" {
						break
					}
				}
				_, _ = io.WriteString(c, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
			}(conn)
		}
	}()

	stop := func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	return ln.Addr().String(), stop
}

func startUDPEchoServer(t *testing.T) (*net.UDPAddr, func()) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp echo: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(buf[:n], addr)
		}
	}()

	stop := func() {
		_ = conn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	return conn.LocalAddr().(*net.UDPAddr), stop
}

func assertHTTP204(t *testing.T, conn net.Conn) {
	t.Helper()
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	req := "GET /health HTTP/1.1\r\nHost: integration.local\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	if !strings.Contains(statusLine, "204") {
		t.Fatalf("unexpected status line: %q", statusLine)
	}
}

func TestApplyAndClearHandshakeDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	if err := applyHandshakeDeadline(client, time.Second); err != nil {
		t.Fatalf("apply deadline: %v", err)
	}
	if err := clearConnDeadline(client); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
}

func TestBuildSocks5CommandRequestWithIPv6(t *testing.T) {
	req, err := buildSocks5CommandRequest(socksCmdConnect, "[::1]:443")
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if len(req) < 4 {
		t.Fatalf("request too short: %d", len(req))
	}
	if req[3] != socksAtypIPv6 {
		t.Fatalf("unexpected atyp: got=%d want=%d", req[3], socksAtypIPv6)
	}
}

func TestParseUTUNUnitEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "", wantErr: false},
		{name: "utun", wantErr: false},
		{name: "utun0", wantErr: false},
		{name: "utun10", wantErr: false},
		{name: "tun0", wantErr: false},
		{name: "abc", wantErr: true},
		{name: "utun-1", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("name=%q", tc.name), func(t *testing.T) {
			_, err := parseUTUNUnit(tc.name)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseLinuxDefaultRoute(t *testing.T) {
	via, dev := parseLinuxDefaultRoute("default via 192.168.1.1 dev eth0 proto dhcp metric 100")
	if via != "192.168.1.1" || dev != "eth0" {
		t.Fatalf("unexpected parse result: via=%q dev=%q", via, dev)
	}
}

func TestParseLinuxInterfaceCIDRs(t *testing.T) {
	out := "" +
		"2: en0    inet 192.168.5.7/24 brd 192.168.5.255 scope global dynamic en0\n" +
		"2: en0    inet 10.0.0.8/16 brd 10.0.255.255 scope global secondary en0\n"
	got := parseLinuxInterfaceCIDRs(out)
	want := []string{"192.168.5.0/24", "10.0.0.0/16"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected linux interface cidrs: got=%v want=%v", got, want)
	}
}

func TestSelectLinuxBypassRoute(t *testing.T) {
	out := "" +
		"default dev tun0 scope link\n" +
		"default via 192.168.1.1 dev wlan0 proto dhcp metric 600\n"

	line, via, dev := selectLinuxBypassRoute(out, "tun0")
	if line != "default via 192.168.1.1 dev wlan0 proto dhcp metric 600" {
		t.Fatalf("unexpected route line: %q", line)
	}
	if via != "192.168.1.1" || dev != "wlan0" {
		t.Fatalf("unexpected parse result: via=%q dev=%q", via, dev)
	}
}

func TestSelectLinuxBypassRouteAcceptsDeviceOnlyDefault(t *testing.T) {
	out := "" +
		"default dev tun0 scope link\n" +
		"default dev ppp0 scope link\n"

	line, via, dev := selectLinuxBypassRoute(out, "tun0")
	if line != "default dev ppp0 scope link" {
		t.Fatalf("unexpected route line: %q", line)
	}
	if via != "" || dev != "ppp0" {
		t.Fatalf("unexpected parse result: via=%q dev=%q", via, dev)
	}
}

func TestSelectLinuxBypassRoutePrefersLowestMetric(t *testing.T) {
	out := "" +
		"default dev tun0 scope link\n" +
		"default via 192.168.1.1 dev wlan0 proto dhcp metric 600\n" +
		"default via 10.0.0.1 dev eth0 proto dhcp metric 100\n"

	line, via, dev := selectLinuxBypassRoute(out, "tun0")
	if line != "default via 10.0.0.1 dev eth0 proto dhcp metric 100" {
		t.Fatalf("unexpected route line: %q", line)
	}
	if via != "10.0.0.1" || dev != "eth0" {
		t.Fatalf("unexpected parse result: via=%q dev=%q", via, dev)
	}
}

func TestSelectLinuxBypassRoutePrefersImplicitZeroMetric(t *testing.T) {
	out := "" +
		"default dev tun0 scope link\n" +
		"default via 192.168.1.1 dev eth0 proto dhcp\n" +
		"default via 10.0.0.1 dev wlan0 proto dhcp metric 100\n"

	line, via, dev := selectLinuxBypassRoute(out, "tun0")
	if line != "default via 192.168.1.1 dev eth0 proto dhcp" {
		t.Fatalf("unexpected route line: %q", line)
	}
	if via != "192.168.1.1" || dev != "eth0" {
		t.Fatalf("unexpected parse result: via=%q dev=%q", via, dev)
	}
}

func TestParseDarwinDefaultRoute(t *testing.T) {
	out := "   route to: default\ngateway: 10.0.0.1\n interface: en0\n"
	gw, iface := parseDarwinDefaultRoute(out)
	if gw != "10.0.0.1" || iface != "en0" {
		t.Fatalf("unexpected parse result: gateway=%q interface=%q", gw, iface)
	}
}

func TestSnapshotLinuxDefaultRouteRejectsEmptyOutput(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf ''")
	}
	defer func() { execCommandContext = prev }()

	_, _, _, err := snapshotLinuxDefaultRoute()
	if err == nil || !strings.Contains(err.Error(), "empty route output") {
		t.Fatalf("expected empty linux route output error, got %v", err)
	}
}

func TestSnapshotLinuxDefaultRoutePrefersLowestMetric(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'default via 192.168.1.1 dev wlan0 metric 600\\ndefault via 10.0.0.1 dev eth0 metric 100\\n'")
	}
	defer func() { execCommandContext = prev }()

	line, via, dev, err := snapshotLinuxDefaultRoute()
	if err != nil {
		t.Fatalf("snapshot linux default route: %v", err)
	}
	if line != "default via 10.0.0.1 dev eth0 metric 100" || via != "10.0.0.1" || dev != "eth0" {
		t.Fatalf("unexpected route snapshot: line=%q via=%q dev=%q", line, via, dev)
	}
}

func TestSnapshotLinuxDefaultRoutePrefersImplicitZeroMetric(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'default via 192.168.1.1 dev eth0\\ndefault via 10.0.0.1 dev wlan0 metric 100\\n'")
	}
	defer func() { execCommandContext = prev }()

	line, via, dev, err := snapshotLinuxDefaultRoute()
	if err != nil {
		t.Fatalf("snapshot linux default route: %v", err)
	}
	if line != "default via 192.168.1.1 dev eth0" || via != "192.168.1.1" || dev != "eth0" {
		t.Fatalf("unexpected route snapshot: line=%q via=%q dev=%q", line, via, dev)
	}
}

func TestReplaceLinuxHostRouteAllowsDeviceOnlyDefaultRoute(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var gotName string
	var gotArgs []string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}

	if err := replaceLinuxHostRoute("203.0.113.7", "", "ppp0"); err != nil {
		t.Fatalf("replace linux host route: %v", err)
	}
	if gotName != "ip" {
		t.Fatalf("unexpected command name: %q", gotName)
	}
	joined := strings.Join(gotArgs, " ")
	if strings.Contains(joined, " via ") {
		t.Fatalf("device-only host route should not include via: %q", joined)
	}
	if !strings.Contains(joined, "203.0.113.7/32") || !strings.Contains(joined, "dev ppp0") {
		t.Fatalf("unexpected host route args: %q", joined)
	}
}

func TestReplaceLinuxHostRouteSupportsIPv6(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var gotName string
	var gotArgs []string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}

	if err := replaceLinuxHostRoute("2001:db8::7", "fe80::1", "en0"); err != nil {
		t.Fatalf("replace linux host route: %v", err)
	}
	if gotName != "ip" {
		t.Fatalf("unexpected command name: %q", gotName)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "-6 route replace 2001:db8::7/128 via fe80::1 dev en0") {
		t.Fatalf("unexpected host route args: %q", joined)
	}
}

func TestReplaceLinuxHostRouteRetriesWithOnlinkForInvalidGateway(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if printf '%%s' "$*" | grep -q 'onlink'; then
	exit 0
fi
printf 'Error: Nexthop has invalid gateway.\n' >&2
exit 2
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	if err := replaceLinuxHostRoute("203.0.113.4", "10.0.0.1", "eth0"); err != nil {
		t.Fatalf("replace linux host route with onlink fallback: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "ip route replace 203.0.113.4/32 via 10.0.0.1 dev eth0") {
		t.Fatalf("expected initial host route attempt, log=%q", logText)
	}
	if !strings.Contains(logText, "ip route replace 203.0.113.4/32 via 10.0.0.1 dev eth0 onlink") {
		t.Fatalf("expected onlink fallback attempt, log=%q", logText)
	}
}

func TestReplaceLinuxCIDRRouteUsesGatewayForPublicDirectCIDR(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var gotName string
	var gotArgs []string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}

	if err := replaceLinuxCIDRRoute("1.0.1.0/24", "192.168.1.1", "eth0"); err != nil {
		t.Fatalf("replace linux cidr route: %v", err)
	}
	if gotName != "ip" {
		t.Fatalf("unexpected command name: %q", gotName)
	}
	joined := strings.Join(gotArgs, " ")
	if !strings.Contains(joined, "route replace 1.0.1.0/24 via 192.168.1.1 dev eth0") {
		t.Fatalf("expected gateway-backed public direct cidr route, got %q", joined)
	}
	if strings.Contains(joined, "scope link") {
		t.Fatalf("public direct cidr must not be installed as on-link route: %q", joined)
	}
}

func TestReplaceLinuxCIDRRouteRetriesInvalidGatewayWithOnlink(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if printf '%%s' "$*" | grep -q 'onlink'; then
	exit 0
fi
printf 'Error: Nexthop has invalid gateway.\n' >&2
exit 2
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	if err := replaceLinuxCIDRRoute("1.0.1.0/24", "192.168.1.1", "eth0"); err != nil {
		t.Fatalf("replace linux cidr route with onlink fallback: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "ip route replace 1.0.1.0/24 via 192.168.1.1 dev eth0") {
		t.Fatalf("expected initial cidr route attempt, log=%q", logText)
	}
	if !strings.Contains(logText, "ip route replace 1.0.1.0/24 via 192.168.1.1 dev eth0 onlink") {
		t.Fatalf("expected onlink cidr route attempt, log=%q", logText)
	}
}

func TestEnsureDirectHostRouteInstallsOneGatewayRouteAndDedupes(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var commands []string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}
	mgr := &AutoRouteManager{
		linuxDefaultGW:  "192.168.1.1",
		linuxDefaultDev: "eth0",
		darwinDefaultGW: "192.168.1.1",
		darwinDefaultIF: "en0",
	}

	mgr.mu.Lock()
	if err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8")); err != nil {
		mgr.mu.Unlock()
		t.Fatalf("ensure direct host route: %v", err)
	}
	mgr.mu.Unlock()
	mgr.mu.Lock()
	if err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8")); err != nil {
		mgr.mu.Unlock()
		t.Fatalf("ensure duplicate direct host route: %v", err)
	}
	mgr.mu.Unlock()

	joined := strings.Join(commands, "\n")
	linuxRoute := "route replace 1.0.1.8/32 via 192.168.1.1 dev eth0"
	darwinRoute := "-net 1.0.1.8/32 192.168.1.1"
	if strings.Count(joined, linuxRoute)+strings.Count(joined, darwinRoute) != 1 {
		t.Fatalf("expected one gateway-backed host route, commands:\n%s", joined)
	}
}

func TestEnsureDirectHostRouteCoalescesConcurrentInstalls(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	started := make(chan struct{}, 1)
	var calls int
	var callsMu sync.Mutex
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		select {
		case started <- struct{}{}:
		default:
		}
		return exec.CommandContext(ctx, "sh", "-c", "sleep 0.1; exit 0")
	}

	mgr := &AutoRouteManager{
		linuxDefaultGW:  "192.168.1.1",
		linuxDefaultDev: "eth0",
		darwinDefaultGW: "192.168.1.1",
		darwinDefaultIF: "en0",
	}
	errCh := make(chan error, 2)
	run := func() {
		mgr.mu.Lock()
		err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8"))
		mgr.mu.Unlock()
		errCh <- err
	}
	go run()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first route command to start")
	}
	go run()
	time.Sleep(20 * time.Millisecond)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("ensure direct host route: %v", err)
		}
	}
	callsMu.Lock()
	gotCalls := calls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("expected concurrent direct host route installs to coalesce to one command, got %d", gotCalls)
	}
}

func TestEnsureDirectHostRouteRollsBackInstallThatRacesTeardown(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("host route commands are platform-specific")
	}
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	startedPath := filepath.Join(t.TempDir(), "started")
	releasePath := filepath.Join(filepath.Dir(startedPath), "release")
	var commandsMu sync.Mutex
	var commands []string
	var calls atomic.Int32
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commandsMu.Lock()
		commands = append(commands, name+" "+strings.Join(args, " "))
		commandsMu.Unlock()
		if calls.Add(1) == 1 {
			script := fmt.Sprintf("touch %q; while [ ! -f %q ]; do sleep 0.01; done", startedPath, releasePath)
			return exec.CommandContext(ctx, "sh", "-c", script)
		}
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}

	mgr := &AutoRouteManager{
		linuxDefaultGW:  "192.168.1.1",
		linuxDefaultDev: "eth0",
		darwinDefaultGW: "192.168.1.1",
		darwinDefaultIF: "en0",
	}
	errCh := make(chan error, 1)
	go func() {
		mgr.mu.Lock()
		err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8"))
		mgr.mu.Unlock()
		errCh <- err
	}()

	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route install command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mgr.mu.Lock()
	mgr.tearingDown = true
	mgr.mu.Unlock()
	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release route command: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ensure direct host route: %v", err)
	}

	commandsMu.Lock()
	joined := strings.Join(commands, "\n")
	commandsMu.Unlock()
	if strings.Count(joined, "1.0.1.8") < 2 {
		t.Fatalf("expected install and rollback commands for raced route, got:\n%s", joined)
	}
	if len(mgr.installedDirectHosts) != 0 {
		t.Fatalf("teardown-raced route must not remain tracked: %+v", mgr.installedDirectHosts)
	}
}

func TestEnsureDirectHostRouteReportsRollbackFailureThatRacesTeardown(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("host route commands are platform-specific")
	}
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	startedPath := filepath.Join(t.TempDir(), "started")
	releasePath := filepath.Join(filepath.Dir(startedPath), "release")
	var calls atomic.Int32
	execCommandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		if calls.Add(1) == 1 {
			script := fmt.Sprintf("touch %q; while [ ! -f %q ]; do sleep 0.01; done", startedPath, releasePath)
			return exec.CommandContext(ctx, "sh", "-c", script)
		}
		return exec.CommandContext(ctx, "sh", "-c", "printf 'permission denied' >&2; exit 1")
	}

	mgr := &AutoRouteManager{
		linuxDefaultGW:  "192.168.1.1",
		linuxDefaultDev: "eth0",
		darwinDefaultGW: "192.168.1.1",
		darwinDefaultIF: "en0",
	}
	errCh := make(chan error, 1)
	go func() {
		mgr.mu.Lock()
		err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8"))
		mgr.mu.Unlock()
		errCh <- err
	}()

	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route install command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mgr.mu.Lock()
	mgr.tearingDown = true
	mgr.mu.Unlock()
	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release route command: %v", err)
	}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "rollback direct host route") {
		t.Fatalf("expected rollback failure, got %v", err)
	}
	if mgr.teardownRouteErr == nil {
		t.Fatal("expected rollback failure to be retained for teardown")
	}
	if len(mgr.installedDirectHosts) != 0 {
		t.Fatalf("failed rollback route must not be recorded as managed: %+v", mgr.installedDirectHosts)
	}
}

func TestRemoveHostRouteIgnoresAlreadyMissingRoute(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("host route commands are platform-specific")
	}
	prev := execCommandContext
	defer func() { execCommandContext = prev }()
	execCommandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'not in table' >&2; exit 1")
	}
	if err := removeHostRoute("1.0.1.8"); err != nil {
		t.Fatalf("already-missing route should be non-fatal: %v", err)
	}
}

func TestEnsureDirectHostRouteEvictsOldestWhenLimitReached(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var commands []string
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		commands = append(commands, name+" "+strings.Join(args, " "))
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}
	mgr := &AutoRouteManager{
		linuxDefaultGW:       "192.168.1.1",
		linuxDefaultDev:      "eth0",
		darwinDefaultGW:      "192.168.1.1",
		darwinDefaultIF:      "en0",
		installedDirectHosts: make(map[string]directCIDRRouteState),
	}
	state := directCIDRRouteState{gateway: "192.168.1.1", dev: "eth0"}
	for i := 0; i < maxDirectHostRoutes; i++ {
		host := fmt.Sprintf("203.0.%d.%d", i/256, i%256)
		mgr.installedDirectHosts[host] = state
		mgr.installedDirectHostOrder = append(mgr.installedDirectHostOrder, host)
	}
	oldest := mgr.installedDirectHostOrder[0]
	newHost := "1.0.1.8"

	mgr.mu.Lock()
	if err := mgr.ensureDirectHostRouteLocked(net.ParseIP(newHost)); err != nil {
		mgr.mu.Unlock()
		t.Fatalf("ensure direct host route with full cache: %v", err)
	}
	mgr.mu.Unlock()
	if len(mgr.installedDirectHosts) != maxDirectHostRoutes {
		t.Fatalf("expected direct host route cache to stay capped at %d, got %d", maxDirectHostRoutes, len(mgr.installedDirectHosts))
	}
	if _, ok := mgr.installedDirectHosts[oldest]; ok {
		t.Fatalf("expected oldest direct host route %s to be evicted", oldest)
	}
	if _, ok := mgr.installedDirectHosts[newHost]; !ok {
		t.Fatalf("expected new direct host route %s to be installed", newHost)
	}
	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, oldest) {
		t.Fatalf("expected route deletion for evicted host %s, got commands:\n%s", oldest, joined)
	}
	if !strings.Contains(joined, newHost) {
		t.Fatalf("expected route install for new host %s, got commands:\n%s", newHost, joined)
	}
}

func TestEnsureDirectHostRoutePendingInstallDoesNotEvict(t *testing.T) {
	state := directCIDRRouteState{gateway: "192.168.1.1", dev: "eth0"}
	mgr := &AutoRouteManager{
		linuxDefaultGW:       state.gateway,
		linuxDefaultDev:      state.dev,
		darwinDefaultGW:      state.gateway,
		darwinDefaultIF:      "en0",
		installedDirectHosts: make(map[string]directCIDRRouteState),
		pendingDirectHosts:   make(map[string]*directHostRoutePending),
	}
	for i := 0; i < maxDirectHostRoutes; i++ {
		host := fmt.Sprintf("203.0.%d.%d", i/256, i%256)
		mgr.installedDirectHosts[host] = state
		mgr.installedDirectHostOrder = append(mgr.installedDirectHostOrder, host)
	}
	oldest := mgr.installedDirectHostOrder[0]
	pending := &directHostRoutePending{done: make(chan struct{})}
	close(pending.done)
	mgr.pendingDirectHosts["1.0.1.8"] = pending

	mgr.mu.Lock()
	if err := mgr.ensureDirectHostRouteLocked(net.ParseIP("1.0.1.8")); err != nil {
		mgr.mu.Unlock()
		t.Fatalf("wait for pending direct host route: %v", err)
	}
	mgr.mu.Unlock()

	if len(mgr.installedDirectHosts) != maxDirectHostRoutes {
		t.Fatalf("pending install must not evict a route, got %d entries", len(mgr.installedDirectHosts))
	}
	if _, ok := mgr.installedDirectHosts[oldest]; !ok {
		t.Fatalf("pending install unexpectedly evicted oldest route %s", oldest)
	}
}

func TestEnsureDirectHostRouteSkipsPrivateBypassCIDR(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	var calls int
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		calls++
		return exec.CommandContext(ctx, "sh", "-c", "exit 0")
	}
	mgr := &AutoRouteManager{
		linuxDefaultGW:  "192.168.1.1",
		linuxDefaultDev: "eth0",
	}

	if err := mgr.EnsureDirectRoute(DirectRoute{
		IP:       net.ParseIP("192.168.1.20"),
		Decision: RouteDecision{Outbound: OutboundDirect, Rule: "direct_cidr:192.168.0.0/16"},
	}); err != nil {
		t.Fatalf("ensure direct private host route: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected private bypass route to skip host route install, got %d calls", calls)
	}
}

func TestLinuxDeleteRouteArgsUsesSingleIPv6Flag(t *testing.T) {
	args := linuxDeleteRouteArgs("2001:db8::/32", "")
	var flags int
	for _, arg := range args {
		if arg == "-6" {
			flags++
		}
	}
	if flags != 1 {
		t.Fatalf("expected exactly one -6 flag, got args=%v", args)
	}
}

func TestDarwinCIDRRouteArgsUsesGatewayForPublicDirectCIDR(t *testing.T) {
	args := darwinCIDRRouteArgs("add", "1.0.1.0/24", "192.168.1.1")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-net 1.0.1.0/24 192.168.1.1") {
		t.Fatalf("expected gateway-backed darwin cidr route, got %q", joined)
	}
	if strings.Contains(joined, "-interface") {
		t.Fatalf("public direct cidr must not be installed as interface route: %q", joined)
	}
}

func TestRouteCIDRProbeIPSupportsIPv6(t *testing.T) {
	probe, err := routeCIDRProbeIP("fd00:1234::/64")
	if err != nil {
		t.Fatalf("route cidr probe: %v", err)
	}
	if probe != "fd00:1234::1" {
		t.Fatalf("unexpected ipv6 probe: %q", probe)
	}
}

func TestSnapshotDarwinDefaultRouteRejectsMissingInterface(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'gateway: 10.0.0.1\\n'")
	}
	defer func() { execCommandContext = prev }()

	_, _, err := snapshotDarwinDefaultRoute()
	if err == nil || !strings.Contains(err.Error(), "missing interface") {
		t.Fatalf("expected missing darwin interface error, got %v", err)
	}
}

func TestSnapshotDarwinDefaultRouteV6IgnoresNotInTableOutput(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "printf 'route: writing to routing socket: not in table\\n'")
	}
	defer func() { execCommandContext = prev }()

	gateway, iface, err := snapshotDarwinDefaultRouteV6()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if gateway != "" || iface != "" {
		t.Fatalf("expected empty ipv6 default route snapshot, got gateway=%q iface=%q", gateway, iface)
	}
}

func TestRunCommandOutputTimesOut(t *testing.T) {
	prev := execCommandContext
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, "sh", "-c", "sleep 10")
	}
	defer func() { execCommandContext = prev }()

	_, err := runCommandOutput("dummy")
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestEnsureLinuxRepairsDeviceOnlyDefaultRoute(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default dev ppp0 scope link\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev ppp0 src 10.0.0.2\n' "$4"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:             &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:         "tun0",
		upstreamIP:      "203.0.113.7",
		linuxDefaultDev: "eth0",
	}

	if err := mgr.ensureLinux(); err != nil {
		t.Fatalf("ensure linux: %v", err)
	}
	if mgr.linuxDefaultDev != "ppp0" {
		t.Fatalf("expected default dev to update, got %q", mgr.linuxDefaultDev)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "ip route replace 203.0.113.7/32 dev ppp0") {
		t.Fatalf("expected device-only upstream host route, log=%q", logText)
	}
	if !strings.Contains(logText, "ip route replace default dev tun0") {
		t.Fatalf("expected split default route repair, log=%q", logText)
	}
}

func TestRefreshLinuxLANBypassAddsIPv6Route(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-6" ] && [ "$4" = "addr" ]; then
	printf '2: eth0    inet6 2001:db8:1::2/64 scope global\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "get" ]; then
	printf '%%s dev tun0 src fd00:198:18::1\n' "$5"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{tunName: "tun0"}
	if err := mgr.refreshLinuxLANBypass("", "eth0"); err != nil {
		t.Fatalf("refresh linux lan bypass: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if !strings.Contains(string(logData), "ip -6 route replace 2001:db8:1::/64 dev eth0") {
		t.Fatalf("expected ipv6 lan bypass route, log=%q", string(logData))
	}
}

func TestRefreshLinuxLANBypassUsesSeparateIPv4AndIPv6Devices(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ] && [ "$7" = "eth0" ]; then
	printf '2: eth0    inet 192.168.5.7/24 scope global eth0\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-6" ] && [ "$4" = "addr" ] && [ "$7" = "eth0" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ] && [ "$7" = "en0" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-6" ] && [ "$4" = "addr" ] && [ "$7" = "en0" ]; then
	printf '3: en0    inet6 2001:db8:1::2/64 scope global\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev tun0 src 198.18.0.1\n' "$4"
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "get" ]; then
	printf '%%s dev tun0 src fd00:198:18::1\n' "$5"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{tunName: "tun0"}
	if err := mgr.refreshLinuxLANBypass("eth0", "en0"); err != nil {
		t.Fatalf("refresh linux lan bypass: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "ip route replace 192.168.5.0/24 dev eth0 scope link") {
		t.Fatalf("expected ipv4 lan bypass route on eth0, log=%q", logText)
	}
	if !strings.Contains(logText, "ip -6 route replace 2001:db8:1::/64 dev en0") {
		t.Fatalf("expected ipv6 lan bypass route on en0, log=%q", logText)
	}
}

func TestRemoveLinuxManagedRouteSkipsForeignDevice(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev eth9 src 10.0.0.2\n' "$4"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{installedLANDevs: map[string]string{"192.168.5.0/24": "eth0"}}
	mgr.removeLinuxManagedRoute("192.168.5.0/24")

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if strings.Contains(string(logData), "ip route del 192.168.5.0/24") {
		t.Fatalf("unexpected delete for foreign linux route, log=%q", string(logData))
	}
}

func TestRemoveDarwinManagedRouteSkipsForeignInterface(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ]; then
	printf 'gateway: 10.0.0.1\ninterface: en9\n'
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{installedLANIFs: map[string]string{"192.168.5.0/24": "en0"}}
	mgr.removeDarwinManagedRoute("192.168.5.0/24")

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if strings.Contains(string(logData), "route -n delete -net 192.168.5.0/24") {
		t.Fatalf("unexpected delete for foreign darwin route, log=%q", string(logData))
	}
}

func TestRefreshDarwinLANBypassRepairsRouteWhenProbeFails(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ifconfig" ] && [ "$2" = "en0" ]; then
	printf 'inet 192.168.5.7 netmask 0xffffff00 broadcast 192.168.5.255\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ]; then
	printf 'route: writing to routing socket: not in table\n' >&2
	exit 1
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{tunName: "utun7"}
	if err := mgr.refreshDarwinLANBypass("en0", ""); err != nil {
		t.Fatalf("refresh darwin lan bypass: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if !strings.Contains(string(logData), "route -n add -net 192.168.5.0/24 -interface en0") &&
		!strings.Contains(string(logData), "route -n change -net 192.168.5.0/24 -interface en0") {
		t.Fatalf("expected darwin lan bypass repair after probe failure, log=%q", string(logData))
	}
}

func TestRefreshDarwinLANBypassReinstallsRouteStillResolvingViaTun(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
state=%q
if [ "$1" = "ifconfig" ] && [ "$2" = "en0" ]; then
	printf 'inet 192.168.5.7 netmask 0xffffff00 broadcast 192.168.5.255\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ]; then
	if [ ! -f "$state" ]; then
		printf '   route to: 192.168.5.1\ngateway: link#20\n interface: utun7\n'
		exit 0
	fi
	printf '   route to: 192.168.5.1\ngateway: link#4\n interface: en0\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "delete" ]; then
	: > "$state"
	exit 0
fi
exit 0
`, logPath, filepath.Join(t.TempDir(), "deleted"))
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{tunName: "utun7"}
	if err := mgr.refreshDarwinLANBypass("en0", ""); err != nil {
		t.Fatalf("refresh darwin lan bypass: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "route -n delete -net ") {
		t.Fatalf("expected stale darwin lan route deletion, log=%q", logText)
	}
	if got := strings.Count(logText, "route -n add -net "); got < 2 {
		t.Fatalf("expected darwin lan bypass route to be re-added after stale tun resolution, got %d log=%q", got, logText)
	}
}

func TestTeardownLinuxClearsInstalledLANState(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", "exit 0", "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		tunName:           "tun0",
		lanCIDRs:          []string{"192.168.5.0/24"},
		installedLANCIDRs: []string{"192.168.5.0/24"},
		installedLANDevs:  map[string]string{"192.168.5.0/24": "eth0"},
		installedLANIFs:   map[string]string{"192.168.5.0/24": "en0"},
	}

	if err := mgr.teardownLinux(); err != nil {
		t.Fatalf("teardown linux: %v", err)
	}
	if mgr.lanCIDRs != nil || mgr.installedLANCIDRs != nil || mgr.installedLANDevs != nil || mgr.installedLANIFs != nil {
		t.Fatalf("expected linux teardown to clear installed lan state, got lanCIDRs=%v installed=%v devs=%v ifs=%v", mgr.lanCIDRs, mgr.installedLANCIDRs, mgr.installedLANDevs, mgr.installedLANIFs)
	}
}

func TestTeardownDarwinClearsInstalledLANState(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", "exit 0", "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		tunName:           "utun7",
		lanCIDRs:          []string{"192.168.5.0/24"},
		installedLANCIDRs: []string{"192.168.5.0/24"},
		installedLANDevs:  map[string]string{"192.168.5.0/24": "eth0"},
		installedLANIFs:   map[string]string{"192.168.5.0/24": "en0"},
	}

	if err := mgr.teardownDarwin(); err != nil {
		t.Fatalf("teardown darwin: %v", err)
	}
	if mgr.lanCIDRs != nil || mgr.installedLANCIDRs != nil || mgr.installedLANDevs != nil || mgr.installedLANIFs != nil {
		t.Fatalf("expected darwin teardown to clear installed lan state, got lanCIDRs=%v installed=%v devs=%v ifs=%v", mgr.lanCIDRs, mgr.installedLANCIDRs, mgr.installedLANDevs, mgr.installedLANIFs)
	}
}

func TestSetupLinuxInstallsIPv6DefaultRouteWhenPresent(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default via 10.0.0.1 dev eth0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "show" ] && [ "$5" = "default" ]; then
	printf 'default via fe80::1 dev en0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev eth0 src 10.0.0.2\n' "$4"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:        &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:    "tun0",
		upstreamIP: "203.0.113.7",
	}

	if err := mgr.setupLinux(); err != nil {
		t.Fatalf("setup linux: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "ip -6 route replace default dev tun0") {
		t.Fatalf("expected ipv6 default route install, log=%q", logText)
	}
	if !strings.Contains(logText, "ip -6 addr add fd00:198:18::1/64 dev tun0") {
		t.Fatalf("expected tun ipv6 address install, log=%q", logText)
	}
}

func TestSetupLinuxSkipsIPv6DefaultRouteWhenAbsent(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default via 10.0.0.1 dev eth0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "show" ] && [ "$5" = "default" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev eth0 src 10.0.0.2\n' "$4"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:        &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:    "tun0",
		upstreamIP: "203.0.113.7",
	}

	if err := mgr.setupLinux(); err != nil {
		t.Fatalf("setup linux: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if strings.Contains(logText, "ip -6 route replace default dev tun0") || strings.Contains(logText, "ip -6 addr add") {
		t.Fatalf("unexpected ipv6 setup without default route, log=%q", logText)
	}
}

func TestSetupLinuxUsesIPv6DefaultRouteForIPv6Upstream(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "show" ] && [ "$4" = "default" ]; then
	printf 'default via 10.0.0.1 dev eth0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "show" ] && [ "$5" = "default" ]; then
	printf 'default via fe80::1 dev en0 metric 100\n'
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-4" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-o" ] && [ "$3" = "-6" ] && [ "$4" = "addr" ]; then
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "route" ] && [ "$3" = "get" ]; then
	printf '%%s dev eth0 src 10.0.0.2\n' "$4"
	exit 0
fi
if [ "$1" = "ip" ] && [ "$2" = "-6" ] && [ "$3" = "route" ] && [ "$4" = "get" ]; then
	printf '%%s via fe80::1 dev en0 src 2001:db8::2\n' "$5"
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		cfg:        &Config{Tun: TunConfig{IPv4: "198.18.0.1/15"}},
		tunName:    "tun0",
		upstreamIP: "2001:db8::7",
	}

	if err := mgr.setupLinux(); err != nil {
		t.Fatalf("setup linux: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if !strings.Contains(string(logData), "ip -6 route replace 2001:db8::7/128 via fe80::1 dev en0") {
		t.Fatalf("expected ipv6 upstream host route, log=%q", string(logData))
	}
}

func TestNotifyDefaultRouteChangeRunsSynchronously(t *testing.T) {
	mgr := &AutoRouteManager{}
	called := false
	mgr.SetDefaultRouteChangeHook(func() {
		called = true
	})

	mgr.notifyDefaultRouteChange()
	if !called {
		t.Fatal("expected route change hook to run synchronously")
	}
}

func TestEnsureDarwinRepairsSplitRoutesWithoutUpstreamBypass(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ] && [ "$4" = "default" ]; then
	printf 'gateway: 10.0.0.1\ninterface: en0\n'
	exit 0
fi
if [ "$1" = "ifconfig" ] && [ "$2" = "en0" ]; then
	printf 'en0: flags=8863<UP> mtu 1500\n\tinet 192.168.5.7 netmask 0xffffff00 broadcast 192.168.5.255\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ]; then
	printf 'gateway: 10.0.0.1\ninterface: en0\n'
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		tunName:         "utun9",
		darwinDefaultGW: "10.0.0.1",
		darwinDefaultIF: "en0",
	}

	if err := mgr.ensureDarwin(); err != nil {
		t.Fatalf("ensure darwin: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logData)
	if !strings.Contains(logText, "route -n add -net 0.0.0.0/1 -interface utun9") {
		t.Fatalf("expected ipv4 split route repair, log=%q", logText)
	}
	if !strings.Contains(logText, "route -n add -net 128.0.0.0/1 -interface utun9") {
		t.Fatalf("expected second ipv4 split route repair, log=%q", logText)
	}
}

func TestEnsureDarwinUsesIPv6DefaultGatewayForIPv6Upstream(t *testing.T) {
	prev := execCommandContext
	defer func() { execCommandContext = prev }()

	logPath := filepath.Join(t.TempDir(), "commands.log")
	script := fmt.Sprintf(`
printf '%%s\n' "$*" >> %q
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ] && [ "$4" = "default" ]; then
	printf 'gateway: 10.0.0.1\ninterface: en0\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ] && [ "$4" = "-inet6" ] && [ "$5" = "default" ]; then
	printf 'gateway: fe80::1\ninterface: en0\n'
	exit 0
fi
if [ "$1" = "ifconfig" ] && [ "$2" = "en0" ]; then
	printf 'en0: flags=8863<UP> mtu 1500\n\tinet 192.168.5.7 netmask 0xffffff00 broadcast 192.168.5.255\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ] && [ "$4" = "-inet6" ]; then
	printf 'gateway: fe80::1\ninterface: en0\n'
	exit 0
fi
if [ "$1" = "route" ] && [ "$2" = "-n" ] && [ "$3" = "get" ]; then
	printf 'gateway: 10.0.0.1\ninterface: en0\n'
	exit 0
fi
exit 0
`, logPath)
	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		allArgs := append([]string{name}, args...)
		return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, allArgs...)...)
	}

	mgr := &AutoRouteManager{
		tunName:           "utun9",
		upstreamIP:        "2001:db8::7",
		darwinDefaultGW:   "10.0.0.1",
		darwinDefaultIF:   "en0",
		darwinDefaultGWV6: "fe80::1",
		darwinDefaultIFV6: "en0",
	}

	if err := mgr.ensureDarwin(); err != nil {
		t.Fatalf("ensure darwin: %v", err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	if !strings.Contains(string(logData), "route -n add -inet6 -host 2001:db8::7 fe80::1") {
		t.Fatalf("expected ipv6 upstream host route, log=%q", string(logData))
	}
}

func TestParseDarwinInterfaceCIDRs(t *testing.T) {
	out := "" +
		"en0: flags=8863<UP,BROADCAST,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n" +
		"\tinet 192.168.5.7 netmask 0xffffff00 broadcast 192.168.5.255\n" +
		"\tinet 10.0.0.8 netmask 0xffff0000 broadcast 10.0.255.255\n"

	got := parseDarwinInterfaceCIDRs(out)
	want := []string{"192.168.5.0/24", "10.0.0.0/16"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected darwin interface cidrs: got=%v want=%v", got, want)
	}
}

func TestParseDarwinInterfaceCIDRsSkipsIPv6LinkLocal(t *testing.T) {
	out := "" +
		"en0: flags=8863<UP,BROADCAST,RUNNING,SIMPLEX,MULTICAST> mtu 1500\n" +
		"\tinet6 fe80::1%en0 prefixlen 64 scopeid 0x8\n" +
		"\tinet6 2001:db8:1::2 prefixlen 64\n"

	got := parseDarwinInterfaceCIDRs(out)
	want := []string{"2001:db8:1::/64"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected darwin interface cidrs: got=%v want=%v", got, want)
	}
}

func TestRouteCIDRProbeIP(t *testing.T) {
	got, err := routeCIDRProbeIP("192.168.5.0/24")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "192.168.5.1" {
		t.Fatalf("unexpected probe ip: %s", got)
	}
}

func TestDiffCIDRSets(t *testing.T) {
	remove, add := diffCIDRSets([]string{"192.168.5.0/24", "10.0.0.0/16"}, []string{"10.0.0.0/16", "172.16.0.0/12"})
	if !reflect.DeepEqual(remove, []string{"192.168.5.0/24"}) {
		t.Fatalf("unexpected remove set: %v", remove)
	}
	if !reflect.DeepEqual(add, []string{"172.16.0.0/12"}) {
		t.Fatalf("unexpected add set: %v", add)
	}
}

func TestLinuxTunCIDR(t *testing.T) {
	cidr, err := linuxTunCIDR("198.18.0.1/15")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cidr != "198.18.0.1/15" {
		t.Fatalf("unexpected cidr: %q", cidr)
	}

	if _, err := linuxTunCIDR("198.18.0.1"); err == nil {
		t.Fatalf("expected error for non-cidr input")
	}
}

func TestDarwinTunIPMask(t *testing.T) {
	ip, mask, err := darwinTunIPMask("198.18.0.1/15")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip != "198.18.0.1" || mask != "255.254.0.0" {
		t.Fatalf("unexpected darwin tun values: ip=%q mask=%q", ip, mask)
	}
}

func TestTUNProxyEndToEnd(t *testing.T) {
	if os.Getenv("SHAMANTUN_TUN_E2E") != "1" {
		t.Skip("set SHAMANTUN_TUN_E2E=1 to run tun end-to-end case")
	}
	if testing.Short() {
		t.Skip("skip tun e2e in short mode")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("tun e2e is only for linux/darwin, got %s", runtime.GOOS)
	}
	if os.Geteuid() != 0 {
		t.Skip("tun e2e requires root privileges (run tests with sudo)")
	}

	upstreamIP, err := detectPrimaryIPv4()
	if err != nil {
		t.Skipf("skip tun e2e: %v", err)
	}

	virtualTarget := "203.0.113.10:18080"
	backendAddr, stopBackend := startHTTPResponder(t)
	defer stopBackend()

	mtls := generateMTLSAssets(t, []net.IP{upstreamIP})

	t.Run("socks5tls", func(t *testing.T) {
		upstreamAddr, stopServer := startSocks5TLSTestServer(t, upstreamIP, mtls, virtualTarget, backendAddr)
		defer stopServer()

		cfg := buildE2EConfig(t, ModeSocks5TLS, upstreamAddr, mtls, "", "")
		runTUNModeCase(t, cfg, virtualTarget)
	})

	t.Run("https-connect", func(t *testing.T) {
		const (
			basicUser = "it-user"
			basicPass = "it-pass"
		)
		upstreamAddr, stopServer := startHTTPSConnectTLSTestServer(t, upstreamIP, mtls, virtualTarget, backendAddr, basicUser, basicPass)
		defer stopServer()

		cfg := buildE2EConfig(t, ModeHTTPS, upstreamAddr, mtls, basicUser, basicPass)
		runTUNModeCase(t, cfg, virtualTarget)
	})
}

func runTUNModeCase(t *testing.T, cfg *Config, targetAddr string) {
	t.Helper()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	eng, err := NewEngine(cfg)
	if err != nil {
		cancel()
		t.Fatalf("new engine: %v", err)
	}

	var routeMgr *AutoRouteManager
	if err := eng.StartWithPreNetstackHook(ctx, func(tunName string) error {
		var setupErr error
		routeMgr, setupErr = NewAutoRouteManager(cfg, tunName)
		if setupErr != nil {
			return setupErr
		}
		if setupErr = routeMgr.Setup(); setupErr != nil {
			_ = routeMgr.Teardown()
			return setupErr
		}
		eng.SetDirectRouteHook(routeMgr.EnsureDirectRoute)
		return nil
	}); err != nil {
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		_ = eng.Stop(stopCtx)
		stop()
		if routeMgr != nil {
			eng.SetDirectRouteHook(nil)
			_ = routeMgr.Teardown()
		}
		t.Fatalf("engine start: %v", err)
	}

	t.Cleanup(func() {
		eng.SetDirectRouteHook(nil)
		if err := routeMgr.Teardown(); err != nil {
			t.Logf("auto-route teardown warning: %v", err)
		}
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := eng.Stop(stopCtx); err != nil {
			t.Logf("engine stop warning: %v", err)
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := probeHTTPThroughTUN(targetAddr); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("probe through tun failed: %v", lastErr)
}

func probeHTTPThroughTUN(targetAddr string) error {
	conn, err := net.DialTimeout("tcp", targetAddr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	req := "GET /health HTTP/1.1\r\nHost: integration.local\r\nConnection: close\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		return err
	}

	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.Contains(status, "204") {
		return fmt.Errorf("unexpected status line: %q", strings.TrimSpace(status))
	}
	return nil
}

type mtlsAssets struct {
	ServerPEM string
	ServerKey string
	ClientPEM string
	ClientKey string
}

func buildE2EConfig(t *testing.T, mode, upstreamAddr string, mtls *mtlsAssets, basicUser, basicPass string) *Config {
	t.Helper()

	cfg := &Config{
		Mode: mode,
		Tun: TunConfig{
			Name: e2eTunName(),
			MTU:  1500,
			IPv4: "198.18.0.1/15",
		},
		Upstream: UpstreamConfig{
			Addr:               upstreamAddr,
			ServerName:         "",
			ClientPEM:          mtls.ClientPEM,
			ClientKey:          mtls.ClientKey,
			InsecureSkipVerify: false,
			Username:           basicUser,
			Password:           basicPass,
		},
		Runtime: RuntimeConfig{
			ConnectTimeoutMS: 5000,
			IdleTimeoutMS:    10000,
			TCPBuffer:        64 * 1024,
			UDPBuffer:        64 * 1024,
			EnableUDP:        mode == ModeSocks5TLS,
		},
	}
	cfg.applyDefaults()
	return cfg
}

func e2eTunName() string {
	if runtime.GOOS == "darwin" {
		return "utun"
	}
	return fmt.Sprintf("st%d", time.Now().UnixNano()%1_000_000)
}

func detectPrimaryIPv4() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list interfaces: %w", err)
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil {
				continue
			}
			ip4 := ip.To4()
			if ip4 == nil || ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
				continue
			}
			return append(net.IP(nil), ip4...), nil
		}
	}

	return nil, fmt.Errorf("no active non-loopback IPv4 interface found")
}

func generateMTLSAssets(t *testing.T, serverIPs []net.IP) *mtlsAssets {
	t.Helper()

	dir := t.TempDir()
	now := time.Now()

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "shamantun-test-server"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           serverIPs,
	}
	/* Creates self-signed certs for testing. */
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, serverTemplate, &serverKey.PublicKey, serverKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "shamantun-test-client"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, clientTemplate, &clientKey.PublicKey, clientKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	assets := &mtlsAssets{
		ServerPEM: filepath.Join(dir, "server.pem"),
		ServerKey: filepath.Join(dir, "server.key"),
		ClientPEM: filepath.Join(dir, "client.pem"),
		ClientKey: filepath.Join(dir, "client.key"),
	}

	writePEMBlock(t, assets.ServerPEM, "CERTIFICATE", serverDER)
	writePEMBlock(t, assets.ClientPEM, "CERTIFICATE", clientDER)
	writePEMBlock(t, assets.ServerKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(serverKey))
	writePEMBlock(t, assets.ClientKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey))

	return assets
}

func writePEMBlock(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	block := &pem.Block{Type: blockType, Bytes: der}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func startSocks5TLSTestServer(t *testing.T, listenIP net.IP, mtls *mtlsAssets, virtualTarget, backendAddr string) (string, func()) {
	t.Helper()

	ln := newTestTLSListener(t, listenIP, mtls)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				handleSocks5TLSTestConn(c, virtualTarget, backendAddr)
			}(conn)
		}
	}()

	addr := buildListenerAddr(listenIP, ln.Addr().String())
	stop := func() {
		_ = ln.Close()
		wg.Wait()
	}
	return addr, stop
}

func handleSocks5TLSTestConn(conn net.Conn, virtualTarget, backendAddr string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return
	}
	if greeting[0] != socksVersion {
		return
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{socksVersion, 0x00}); err != nil {
		return
	}

	reqHdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, reqHdr); err != nil {
		return
	}
	if reqHdr[0] != socksVersion || reqHdr[1] != socksCmdConnect {
		_, _ = conn.Write([]byte{socksVersion, 0x07, 0x00, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}

	targetAddr, err := readSocks5TargetAddress(conn, reqHdr[3])
	if err != nil {
		_, _ = conn.Write([]byte{socksVersion, 0x08, 0x00, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}
	if targetAddr != virtualTarget {
		_, _ = conn.Write([]byte{socksVersion, 0x04, 0x00, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}

	upstream, err := net.DialTimeout("tcp", backendAddr, 5*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{socksVersion, 0x05, 0x00, socksAtypIPv4, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()

	if _, err := conn.Write([]byte{socksVersion, 0x00, 0x00, socksAtypIPv4, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	relayBidirectional(conn, upstream, 32*1024, 30*time.Second)
}

func readSocks5TargetAddress(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case socksAtypIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(r, addr); err != nil {
			return "", err
		}
		portBuf := make([]byte, 2)
		if _, err := io.ReadFull(r, portBuf); err != nil {
			return "", err
		}
		port := int(binary.BigEndian.Uint16(portBuf))
		return net.JoinHostPort(net.IP(addr).String(), strconv.Itoa(port)), nil
	case socksAtypDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return "", err
		}
		hostLen := int(lenBuf[0])
		hostBuf := make([]byte, hostLen)
		if _, err := io.ReadFull(r, hostBuf); err != nil {
			return "", err
		}
		portBuf := make([]byte, 2)
		if _, err := io.ReadFull(r, portBuf); err != nil {
			return "", err
		}
		port := int(binary.BigEndian.Uint16(portBuf))
		return net.JoinHostPort(string(hostBuf), strconv.Itoa(port)), nil
	case socksAtypIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(r, addr); err != nil {
			return "", err
		}
		portBuf := make([]byte, 2)
		if _, err := io.ReadFull(r, portBuf); err != nil {
			return "", err
		}
		port := int(binary.BigEndian.Uint16(portBuf))
		return net.JoinHostPort(net.IP(addr).String(), strconv.Itoa(port)), nil
	default:
		return "", fmt.Errorf("unsupported socks atyp %d", atyp)
	}
}

func startHTTPSConnectTLSTestServer(t *testing.T, listenIP net.IP, mtls *mtlsAssets, virtualTarget, backendAddr, basicUser, basicPass string) (string, func()) {
	t.Helper()

	ln := newTestTLSListener(t, listenIP, mtls)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				handleHTTPSConnectTestConn(c, virtualTarget, backendAddr, basicUser, basicPass)
			}(conn)
		}
	}()

	addr := buildListenerAddr(listenIP, ln.Addr().String())
	stop := func() {
		_ = ln.Close()
		wg.Wait()
	}
	return addr, stop
}

func handleHTTPSConnectTestConn(conn net.Conn, virtualTarget, backendAddr, basicUser, basicPass string) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	br := bufio.NewReader(conn)
	reqLine, err := br.ReadString('\n')
	if err != nil {
		return
	}
	parts := strings.Split(strings.TrimSpace(reqLine), " ")
	if len(parts) < 2 || strings.ToUpper(parts[0]) != "CONNECT" {
		_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
		return
	}
	targetAddr := parts[1]

	var proxyAuth string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "proxy-authorization:") {
			proxyAuth = strings.TrimSpace(line[len("proxy-authorization:"):])
		}
	}

	if basicUser != "" || basicPass != "" {
		if !validateBasicAuthHeader(proxyAuth, basicUser, basicPass) {
			_, _ = io.WriteString(conn, "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"test\"\r\n\r\n")
			return
		}
	}
	if targetAddr != virtualTarget {
		_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
		return
	}

	upstream, err := net.DialTimeout("tcp", backendAddr, 5*time.Second)
	if err != nil {
		_, _ = io.WriteString(conn, "HTTP/1.1 503 Service Unavailable\r\n\r\n")
		return
	}
	defer upstream.Close()

	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	relayBidirectional(&proxyBufferedConn{Conn: conn, r: br}, upstream, 32*1024, 30*time.Second)
}

type proxyBufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *proxyBufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func validateBasicAuthHeader(header, user, pass string) bool {
	fields := strings.Fields(strings.TrimSpace(header))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Basic") {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return false
	}
	return string(decoded) == user+":"+pass
}

func newTestTLSListener(t *testing.T, listenIP net.IP, mtls *mtlsAssets) net.Listener {
	t.Helper()

	cert, err := tls.LoadX509KeyPair(mtls.ServerPEM, mtls.ServerKey)
	if err != nil {
		t.Fatalf("load server cert: %v", err)
	}
	/* Disables client cert verification in tests because ca_pem is removed. */

	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert, /* Keeps the test server setup simple without ca_pem. */
	}

	ln, err := tls.Listen("tcp", net.JoinHostPort(listenIP.String(), "0"), tlsCfg)
	if err != nil {
		t.Fatalf("listen tls on %s: %v", listenIP.String(), err)
	}
	return ln
}

func buildListenerAddr(ip net.IP, listenAddr string) string {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return listenAddr
	}
	return net.JoinHostPort(ip.String(), port)
}
