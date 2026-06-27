package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	gstack "gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

/* ----------------------------------------------------------------------------- */
/* Core engine */
/* ----------------------------------------------------------------------------- */

/* Wires TUN + gVisor stack + upstream dialer. */
type Engine struct {
	cfg    *Config
	dialer UpstreamDialer
	router *Router
	tunDev *TunDevice
	ns     *NetstackRuntime

	mu          sync.Mutex
	started     bool
	starting    bool
	startDone   chan struct{}
	startCancel context.CancelFunc
	stopping    bool
	rollbackTun *TunDevice
	stopOnce    sync.Once
	workersOnce sync.Once
	workersDone chan struct{}
	wg          sync.WaitGroup

	routeHookMu   sync.RWMutex
	onDirectRoute func(DirectRoute) error

	handlerSem          chan struct{}
	udpHandlerSem       chan struct{}
	tcpDialSem          chan struct{}
	tcpDialBackoffUntil int64
	tcpTargetMu         sync.Mutex
	tcpTargetActive     map[string]int
	tcpTargetBackoff    map[string]int64
	tcpTargetSuccess    map[string]uint64
	tcpTargetWaiters    int
	udpDialSem          chan struct{}
	udpDialBackoffUntil int64
	flowSeq             uint64

	activeMu        sync.Mutex
	activeUpstreams map[io.Closer]struct{}
	activeLocals    map[io.Closer]struct{}
	/* Serializes active/idle upstream ownership transfers against network resets. */
	upstreamLifecycleMu sync.RWMutex
	networkGeneration   atomic.Uint64
	dialCtxMu           sync.Mutex
	dialCtx             context.Context
	dialCancel          context.CancelFunc
	runCancel           context.CancelFunc
	warmupMu            sync.Mutex
	warmupRunning       bool
	warmupPending       bool

	dnsPoolMu     sync.Mutex
	dnsPool       map[string][]idleDNSConn
	dnsPoolClosed bool

	udpPoolMu     sync.Mutex
	udpPool       map[string][]UDPSession
	udpPoolClosed bool

	udpNoResponseMu      sync.Mutex
	udpNoResponseBackoff map[string]int64
	directUDPActiveMu    sync.Mutex
	directUDPActive      map[string]int

	logThrottleMu     sync.Mutex
	logThrottleStates map[string]logThrottleState

	dnsCacheMu sync.RWMutex
	dnsCache   map[string]dnsCacheEntry
	/* Tracks cached key/response payload bytes; map/runtime overhead remains outside this counter. */
	dnsCacheBytes    int64
	dnsCacheMaxBytes int64

	relayBufPool relayBufferPool
}

type dnsCacheEntry struct {
	Response  []byte
	ExpiresAt time.Time
	SizeBytes int64
}

type idleDNSConn struct {
	conn      net.Conn
	expiresAt time.Time
}

const (
	flowSummaryDurationThreshold = 30 * time.Second
	flowSummaryBytesThreshold    = 1 << 20 /* 1MB */
	udpSummaryPacketsThreshold   = 32
	defaultTCPDialConcurrency    = 64
	defaultUDPDialConcurrency    = 4
	defaultTCPHandlerConcurrency = 512
	defaultUDPHandlerConcurrency = 512
	defaultDNSCacheSize          = 1024
	defaultDNSCacheMaxBytes      = 64 * 1024 * 1024
	tcpDialAcquireTimeout        = 300 * time.Millisecond
	udpDialAcquireTimeout        = 250 * time.Millisecond
	transientTCPDialBudgetCap    = 1200 * time.Millisecond
	transientTCPDialRetryDelay   = 120 * time.Millisecond
	tcpDialCooldownPollInterval  = 25 * time.Millisecond
	logThrottleInterval          = 5 * time.Second
	logThrottleStateTTL          = 30 * time.Second
	logThrottleStateMaxEntries   = 256
	dnsPoolPerTarget             = 8
	dnsPoolMaxIdle               = 32
	dnsPoolIdleTTL               = 30 * time.Second
	dnsCacheExpirySweepEntries   = 32
	udpPoolMaxIdle               = 16
	udpNoResponseBackoffDuration = 60 * time.Second
	udpNoResponseBackoffMax      = 512
	tcpTargetBackoffDuration     = 15 * time.Second
	tcpTargetBackoffMax          = 512
	/* Browsers and DNS resolvers routinely fan out more than eight fresh
	   connections to one destination after an idle-pool miss. Keep this below
	   the global dial cap while allowing a normal 16-connection burst. */
	tcpTargetDialConcurrency     = 16
	tcpTargetDialWaitConcurrency = 32
	tcpTargetDialWaitTimeout     = time.Second
	udpPumpDrainWarningTimeout   = 2 * time.Second
	udpPumpDrainForceTimeout     = 250 * time.Millisecond
	directUDPActiveTargetMax     = 4
	maxPooledRelayBufferBytes    = 256 * 1024
	minPooledRelayBufferBytes    = 4 * 1024
	relayBufferPoolClasses       = 7 /* 4, 8, 16, 32, 64, 128, and 256 KiB. */
)

type LogLevel int32

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelSystem
)

var (
	currentLogLevel atomic.Int32 /* LogLevel */
)

/* Keeps reusable relay buffers isolated by engine and power-of-two size class. */
type relayBufferPool struct {
	classes [relayBufferPoolClasses]sync.Pool
}

type logThrottleState struct {
	LastLogTime time.Time
	Suppressed  int
}

/* Sets the global log filter level. */
func SetLogLevel(level LogLevel) {
	currentLogLevel.Store(int32(level))
}

func logf(level LogLevel, format string, args ...any) {
	if LogLevel(currentLogLevel.Load()) <= level {
		log.Printf(format, args...)
	}
}

func shouldLogTCPSummary(stats bidirectionalRelayStats) bool {
	if hasUnexpectedRelayError(stats.UploadErr) || hasUnexpectedRelayError(stats.DownloadErr) {
		return true
	}
	return stats.UploadBytes+stats.DownloadBytes >= flowSummaryBytesThreshold
}

func hasUnexpectedRelayError(err error) bool {
	if err == nil ||
		errors.Is(err, io.EOF) ||
		isClosedConnError(err) ||
		isBenignLocalEndpointClosedError(err) ||
		isTimeoutError(err) {
		return false
	}
	return true
}

func formatLogError(err error) string {
	if err == nil {
		return "<nil>"
	}
	return strings.Join(strings.Fields(err.Error()), " ")
}

func shouldLogUDPSummary(up, down udpPumpResult, duration time.Duration) bool {
	if isContextCanceledLike(up.Err) && isContextCanceledLike(down.Err) {
		return false
	}
	if udpNoUpstreamResponse(up, down) {
		return true
	}
	if hasUnexpectedUDPPumpError(up) || hasUnexpectedUDPPumpError(down) {
		return true
	}
	return duration >= flowSummaryDurationThreshold ||
		up.Packets+down.Packets >= udpSummaryPacketsThreshold
}

/* Keeps repeated direct UDP probe failures visible without logging every flow in a burst. */
func (e *Engine) logDirectUDPSummary(flowID, source, target, rule string, duration time.Duration, up, down udpPumpResult) {
	suppressed := 0
	if udpNoUpstreamResponse(up, down) {
		const key = "direct|udp-no-response"
		now := time.Now()

		e.logThrottleMu.Lock()
		if e.logThrottleStates == nil {
			e.logThrottleStates = make(map[string]logThrottleState)
		}
		if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
			e.pruneLogThrottleStatesLocked(now)
		}
		if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
			e.logThrottleMu.Unlock()
		} else {
			state := e.logThrottleStates[key]
			if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
				state.Suppressed++
				e.logThrottleStates[key] = state
				e.logThrottleMu.Unlock()
				return
			}
			suppressed = state.Suppressed
			state.LastLogTime = now
			state.Suppressed = 0
			e.logThrottleStates[key] = state
			e.logThrottleMu.Unlock()
		}
	}

	if suppressed > 0 {
		logf(LogLevelInfo,
			"[INFO] [UDP] %s -> %s (%s:%s) id=%s | %s | up %s (%dp) / down %s (%dp) | %s | suppressed=%d",
			source, target, OutboundDirect, rule, flowID,
			formatDuration(duration),
			formatBytes(up.Bytes), up.Packets,
			formatBytes(down.Bytes), down.Packets,
			combineUDPResults(up, down), suppressed,
		)
		return
	}
	logf(LogLevelInfo,
		"[INFO] [UDP] %s -> %s (%s:%s) id=%s | %s | up %s (%dp) / down %s (%dp) | %s",
		source, target, OutboundDirect, rule, flowID,
		formatDuration(duration),
		formatBytes(up.Bytes), up.Packets,
		formatBytes(down.Bytes), down.Packets,
		combineUDPResults(up, down),
	)
}

func hasUnexpectedUDPPumpError(result udpPumpResult) bool {
	err := result.Err
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		isClosedConnError(err) || isBenignLocalEndpointClosedError(err) ||
		isTimeoutError(err) || isContextCanceledLike(err) {
		return false
	}
	return !result.LocalWriteErr || !isBenignLocalPacketWriteError(err)
}

/* Builds a cache key from the target and the DNS payload with the transaction ID stripped. */
func dnsCacheKey(target string, payload []byte) string {
	if len(payload) <= 2 {
		return target + "|"
	}
	return target + "|" + string(payload[2:])
}

func dnsQuestionIdentity(payload []byte) string {
	offset, ok := dnsQuestionSectionEnd(payload)
	if !ok {
		return ""
	}
	buf := make([]byte, 0, 2+offset-12)
	buf = append(buf, payload[4:6]...)
	buf = append(buf, payload[12:offset]...)
	return string(buf)
}

func dnsQuestionSectionEnd(payload []byte) (int, bool) {
	if len(payload) < 12 {
		return 0, false
	}
	qdcount := int(binary.BigEndian.Uint16(payload[4:6]))
	offset := 12
	for i := 0; i < qdcount; i++ {
		for offset < len(payload) {
			b := payload[offset]
			offset++
			if b == 0 {
				break
			}
			if b&0xC0 == 0xC0 {
				if offset >= len(payload) {
					return 0, false
				}
				offset++
				break
			}
			offset += int(b)
			if offset > len(payload) {
				return 0, false
			}
		}
		if offset+4 > len(payload) {
			return 0, false
		}
		offset += 4
	}
	return offset, true
}

/* Builds the runtime engine from user config. */
func NewEngine(cfg *Config) (*Engine, error) {
	if cfg == nil {
		return nil, errors.New("nil config")
	}
	cfg.applyDefaults()
	dialer, err := NewUpstreamDialerSet(cfg.Mode, cfg.Upstreams, cfg.ConnectTimeout(), cfg.Runtime.UDPBuffer)
	if err != nil {
		return nil, err
	}
	router, err := NewRouter(cfg.Routing)
	if err != nil {
		return nil, err
	}
	tcpConc := cfg.Runtime.TCPDialConcurrency
	if tcpConc <= 0 {
		tcpConc = defaultTCPDialConcurrency
	}
	udpConc := cfg.Runtime.UDPDialConcurrency
	if udpConc <= 0 {
		udpConc = defaultUDPDialConcurrency
	}
	tcpHandlerConc := cfg.Runtime.TCPHandlerConcurrency
	if tcpHandlerConc <= 0 {
		tcpHandlerConc = defaultTCPHandlerConcurrency
	}
	udpHandlerConc := cfg.Runtime.UDPHandlerConcurrency
	if udpHandlerConc <= 0 {
		udpHandlerConc = defaultUDPHandlerConcurrency
	}
	dnsCacheSize := cfg.Runtime.DNSCacheSize
	if dnsCacheSize <= 0 {
		dnsCacheSize = defaultDNSCacheSize
	}
	dialCtx, dialCancel := context.WithCancel(context.Background())
	return &Engine{
		cfg:                  cfg,
		dialer:               dialer,
		router:               router,
		handlerSem:           make(chan struct{}, tcpHandlerConc),
		udpHandlerSem:        make(chan struct{}, udpHandlerConc),
		tcpDialSem:           make(chan struct{}, tcpConc),
		tcpTargetActive:      make(map[string]int),
		tcpTargetBackoff:     make(map[string]int64),
		tcpTargetSuccess:     make(map[string]uint64),
		udpDialSem:           make(chan struct{}, udpConc),
		activeUpstreams:      make(map[io.Closer]struct{}),
		activeLocals:         make(map[io.Closer]struct{}),
		dnsPool:              make(map[string][]idleDNSConn),
		dnsCache:             make(map[string]dnsCacheEntry, dnsCacheSize),
		udpPool:              make(map[string][]UDPSession),
		udpNoResponseBackoff: make(map[string]int64),
		directUDPActive:      make(map[string]int),
		logThrottleStates:    make(map[string]logThrottleState),
		dialCtx:              dialCtx,
		dialCancel:           dialCancel,
	}, nil
}

func (e *Engine) SetDirectRouteHook(fn func(DirectRoute) error) {
	e.routeHookMu.Lock()
	e.onDirectRoute = fn
	e.routeHookMu.Unlock()
}

/* Initializes TUN and netstack, then registers flow handlers. */
func (e *Engine) Start(ctx context.Context) error {
	return e.start(ctx, nil)
}

func (e *Engine) StartWithPreNetstackHook(ctx context.Context, hook func(tunName string) error) error {
	return e.start(ctx, hook)
}

func (e *Engine) start(ctx context.Context, preNetstackHook func(tunName string) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e == nil {
		return errors.New("nil engine")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	switch {
	case e.starting:
		e.mu.Unlock()
		return errors.New("engine start already in progress")
	case e.rollbackTun != nil:
		e.mu.Unlock()
		return errors.New("engine cleanup in progress")
	case e.stopping:
		e.mu.Unlock()
		return errors.New("engine already stopped")
	case e.started:
		e.mu.Unlock()
		return errors.New("engine already started")
	}
	e.starting = true
	e.startDone = make(chan struct{})
	startDone := e.startDone
	startCtx, startCancel := context.WithCancel(ctx)
	e.startCancel = startCancel
	e.mu.Unlock()
	if e.cfg == nil {
		e.mu.Lock()
		e.starting = false
		if e.startDone == startDone {
			e.startDone = nil
		}
		e.startCancel = nil
		e.mu.Unlock()
		startCancel()
		close(startDone)
		return errors.New("nil config")
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	started := false
	defer func() {
		if started {
			return
		}
		runCancel()
		e.mu.Lock()
		e.starting = false
		if e.startDone == startDone {
			e.startDone = nil
		}
		e.startCancel = nil
		e.mu.Unlock()
		startCancel()
		close(startDone)
	}()

	tunDev, err := OpenTUN(e.cfg.Tun.Name, e.cfg.Tun.MTU)
	if err != nil {
		return err
	}
	if err := startCtx.Err(); err != nil {
		_ = e.closeStartupTun(startCtx, tunDev)
		return err
	}

	logf(LogLevelSystem, "[SYS] [TUN] tun opened: name=%s mtu=%d", tunDev.Name, tunDev.MTU)
	if preNetstackHook != nil {
		if err := preNetstackHook(tunDev.Name); err != nil {
			_ = e.closeStartupTun(startCtx, tunDev)
			return err
		}
	}
	if e.cfg.Runtime.EnableUDP {
		logf(LogLevelDebug,
			"[DEBUG] [UDP] runtime policy: udp_forwarding=true udp_dial_concurrency=%d tcp_dial_concurrency=%d tcp_handler_concurrency=%d udp_handler_concurrency=%d",
			cap(e.udpDialSem),
			cap(e.tcpDialSem),
			cap(e.handlerSem),
			cap(e.udpHandlerSem),
		)
	}

	ns, err := NewNetstack(tunDev.LinkEndpoint, NetstackOptions{
		EnableUDP: e.cfg.Runtime.EnableUDP,
		TCPHandler: func(id gstack.TransportEndpointID, conn net.Conn) {
			if !e.acquireHandlerSlot() {
				logf(LogLevelSystem, "[SYS] [TCP] handler concurrency limit reached (%d); dropping connection from %s", cap(e.handlerSem), id.RemoteAddress)
				_ = conn.Close()
				return
			}
			defer e.releaseHandlerSlot()
			if !e.beginWorker() {
				_ = conn.Close()
				return
			}
			defer e.wg.Done()
			e.handleTCP(runCtx, id, conn)
		},
		UDPHandler: func(id gstack.TransportEndpointID, conn net.PacketConn) {
			if !e.acquireUDPHandlerSlot() {
				e.logUDPHandlerLimit(id.RemoteAddress.String())
				_ = conn.Close()
				return
			}
			defer e.releaseUDPHandlerSlot()
			if !e.beginWorker() {
				_ = conn.Close()
				return
			}
			defer e.wg.Done()
			e.handleUDP(runCtx, id, conn)
		},
	})
	if err != nil {
		_ = e.closeStartupTun(startCtx, tunDev)
		return err
	}
	if err := startCtx.Err(); err != nil {
		if ns != nil && ns.Stack != nil {
			ns.Stack.Close()
		}
		_ = e.closeStartupTun(startCtx, tunDev)
		return err
	}

	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		if ns != nil && ns.Stack != nil {
			ns.Stack.Close()
		}
		_ = e.closeStartupTun(startCtx, tunDev)
		return errors.New("engine stopped during startup")
	}
	e.tunDev = tunDev
	e.ns = ns
	e.runCancel = runCancel
	e.started = true
	e.starting = false
	if e.startDone == startDone {
		e.startDone = nil
	}
	e.startCancel = nil
	e.mu.Unlock()
	startCancel()
	close(startDone)
	started = true

	return nil
}

/* Closes ingress/egress paths and waits for in-flight workers to exit. */
func (e *Engine) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e == nil {
		return errors.New("nil engine")
	}
	e.mu.Lock()
	rollbackTun := e.rollbackTun
	starting := e.starting
	startDone := e.startDone
	startCancel := e.startCancel
	neverStarted := !e.started && !starting && !e.stopping && e.tunDev == nil && e.ns == nil && rollbackTun == nil
	e.mu.Unlock()
	if starting && startDone != nil {
		e.mu.Lock()
		e.stopping = true
		e.mu.Unlock()
		if startCancel != nil {
			startCancel()
		}
		select {
		case <-startDone:
			return e.Stop(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if neverStarted {
		return nil
	}
	if rollbackTun != nil {
		err := rollbackTun.CloseContext(ctx)
		if !isContextCancelLike(err) {
			e.clearRollbackTun(rollbackTun)
		}
		return err
	}

	var retErr error
	e.Quiesce()

	e.mu.Lock()
	tunDev := e.tunDev
	e.mu.Unlock()
	if tunDev != nil {
		if err := tunDev.CloseContext(ctx); err != nil {
			if retErr != nil {
				retErr = errors.Join(retErr, err)
			} else {
				retErr = err
			}
			if ctx.Err() != nil {
				return retErr
			}
		}
	}

	select {
	case <-e.workerWaitDone():
		/* Defense in depth: permanent pool closure makes these drains normally empty. */
		e.closeDNSPool()
		e.closeUDPPool()
	case <-ctx.Done():
		if retErr != nil {
			retErr = errors.Join(retErr, ctx.Err())
		} else {
			retErr = ctx.Err()
		}
	}

	return retErr
}

/* Immediately closes packet ingress and active relay paths without waiting for teardown. */
func (e *Engine) Quiesce() {
	if e == nil {
		return
	}
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.stopping = true
		runCancel := e.runCancel
		e.mu.Unlock()
		if runCancel != nil {
			runCancel()
		}
		e.upstreamLifecycleMu.Lock()
		e.resetDialContext()
		e.closeDNSPool()
		e.closeUDPPool()
		e.closeActiveUpstreams()
		e.upstreamLifecycleMu.Unlock()
		e.closeActiveLocals()

		if e.ns != nil && e.ns.Stack != nil {
			e.ns.Stack.Close()
		}
	})
}

func (e *Engine) workerWaitDone() <-chan struct{} {
	e.mu.Lock()
	if e.workersDone == nil {
		e.workersDone = make(chan struct{})
	}
	done := e.workersDone
	e.mu.Unlock()
	e.workersOnce.Do(func() {
		go func() {
			e.wg.Wait()
			close(done)
		}()
	})
	return done
}

func (e *Engine) closeStartupTun(ctx context.Context, tunDev *TunDevice) error {
	if e == nil || tunDev == nil {
		return nil
	}
	e.mu.Lock()
	e.rollbackTun = tunDev
	e.mu.Unlock()

	err := tunDev.CloseContext(ctx)
	if isContextCancelLike(err) {
		/* The caller's context expired before the TUN device closed. Kick cleanup into a
		   background goroutine so the engine is not permanently wedged in "cleanup in
		   progress". Note: if the kernel blocks file.Close() (e.g. TUN driver bug),
		   this goroutine will hang and rollbackTun will remain set until the process exits.
		   The TUN file is opened in blocking mode so no deadline shortcut is available. */
		go func(dev *TunDevice) {
			_ = dev.CloseContext(context.Background())
			e.clearRollbackTun(dev)
		}(tunDev)
		return err
	}
	e.clearRollbackTun(tunDev)
	return err
}

func (e *Engine) clearRollbackTun(tunDev *TunDevice) {
	if e == nil || tunDev == nil {
		return
	}
	e.mu.Lock()
	if e.rollbackTun == tunDev {
		e.rollbackTun = nil
	}
	e.mu.Unlock()
}

func isContextCancelLike(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "operation was canceled")
}

func isContextCanceledLike(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return errors.Is(err, context.Canceled) ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "operation was canceled")
}

/* Guards worker scheduling during shutdown. */
func (e *Engine) beginWorker() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopping {
		return false
	}
	e.wg.Add(1)
	return true
}

/* Claims a handler concurrency slot without blocking; returns false if all slots are in use. */
func (e *Engine) acquireHandlerSlot() bool {
	select {
	case e.handlerSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *Engine) releaseHandlerSlot() {
	select {
	case <-e.handlerSem:
	default:
	}
}

func (e *Engine) acquireUDPHandlerSlot() bool {
	select {
	case e.udpHandlerSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *Engine) releaseUDPHandlerSlot() {
	select {
	case <-e.udpHandlerSem:
	default:
	}
}

func (e *Engine) activateLocal(closer io.Closer) bool {
	if closer == nil {
		return false
	}
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	if stopping {
		_ = closer.Close()
		return false
	}
	e.registerActiveLocal(closer)
	e.mu.Lock()
	stopping = e.stopping
	e.mu.Unlock()
	if stopping {
		e.unregisterActiveLocal(closer)
		_ = closer.Close()
		return false
	}
	return true
}

func (e *Engine) activateUpstream(closer io.Closer) bool {
	if closer == nil {
		return false
	}
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	if stopping {
		_ = closer.Close()
		return false
	}
	e.registerActiveUpstream(closer)
	e.mu.Lock()
	stopping = e.stopping
	e.mu.Unlock()
	if stopping {
		e.unregisterActiveUpstream(closer)
		_ = closer.Close()
		return false
	}
	return true
}

func (e *Engine) activateUpstreamForGeneration(closer io.Closer, generation uint64) bool {
	if closer == nil {
		return false
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		_ = closer.Close()
		return false
	}
	return e.activateUpstream(closer)
}

/* Bridges one TCP flow between local netstack and upstream proxy. */
func (e *Engine) handleTCP(ctx context.Context, id gstack.TransportEndpointID, localConn net.Conn) {
	defer localConn.Close()
	if !e.activateLocal(localConn) {
		return
	}
	defer e.unregisterActiveLocal(localConn)

	flowID := e.nextFlowID("tcp")
	target := tcpDestination(id)
	source := tcpSource(id)
	if isAnyUpstreamLoopTarget(target, e.cfg.Upstreams) {
		logf(LogLevelWarn, "[WARN] [TCP] [%s] dropped: detected upstream route loop %s -> %s | hint: check auto-route upstream bypass", flowID, source, target)
		return
	}
	decision := e.routeDecisionForTarget(target)
	if decision.Outbound == OutboundDirect {
		if err := e.ensureDirectRouteForTarget(target, decision); err != nil {
			e.logTCPDialFailure(flowID, OutboundDirect, source, target, err)
			return
		}
		e.handleDirectTCP(ctx, flowID, source, target, localConn, decision)
		return
	}
	upConn, err := e.dialTCPUpstream(ctx, target, e.tcpConnectBudget())
	if err != nil {
		if errors.Is(err, context.Canceled) || (errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil) {
			logf(LogLevelDebug, "[DEBUG] [TCP] %s -> %s (%s) id=%s | dial aborted during shutdown", source, target, e.dialer.Mode(), flowID)
		} else if errors.Is(err, errTCPDialCooldown) {
			logf(LogLevelDebug, "[DEBUG] [TCP] %s -> %s (%s) id=%s | dropped: dial cooldown", source, target, e.dialer.Mode(), flowID)
		} else if errors.Is(err, errTCPDialThrottle) {
			logf(LogLevelDebug, "[DEBUG] [TCP] %s -> %s (%s) id=%s | dropped: dial throttle", source, target, e.dialer.Mode(), flowID)
		} else if errors.Is(err, errTCPTargetCooldown) {
			e.logTCPTargetPressure(flowID, e.dialer.Mode(), source, target, "target cooldown")
		} else if errors.Is(err, errTCPTargetThrottle) {
			e.logTCPTargetPressure(flowID, e.dialer.Mode(), source, target, "target dial limit")
		} else {
			e.logTCPDialFailure(flowID, e.dialer.Mode(), source, target, err)
		}
		return
	}
	defer e.unregisterActiveUpstream(upConn)
	logf(LogLevelDebug, "[DEBUG] [TCP] %s -> %s (%s) id=%s | relay start", source, target, e.dialer.Mode(), flowID)
	stats := relayBidirectionalWithStatsContext(ctx, localConn, upConn, e.cfg.Runtime.TCPBuffer, e.cfg.IdleTimeout(), &e.relayBufPool)
	if shouldLogTCPSummary(stats) {
		logf(LogLevelInfo,
			"[INFO] [TCP] %s -> %s (%s) id=%s | %s | up %s / down %s | %s",
			source, target, e.dialer.Mode(), flowID,
			formatDuration(stats.Duration),
			formatBytes(stats.UploadBytes), formatBytes(stats.DownloadBytes),
			combineResults(stats.UploadErr, stats.DownloadErr),
		)
	}
}

func (e *Engine) handleDirectTCP(ctx context.Context, flowID, source, target string, localConn net.Conn, decision RouteDecision) {
	upConn, err := e.dialDirectTCP(ctx, target, e.tcpConnectBudget())
	if err != nil {
		e.logTCPDialFailure(flowID, OutboundDirect, source, target, err)
		return
	}
	defer e.unregisterActiveUpstream(upConn)
	logf(LogLevelDebug, "[DEBUG] [TCP] %s -> %s (%s:%s) id=%s | relay start", source, target, OutboundDirect, decision.Rule, flowID)
	stats := relayBidirectionalWithStatsContext(ctx, localConn, upConn, e.cfg.Runtime.TCPBuffer, e.cfg.IdleTimeout(), &e.relayBufPool)
	if shouldLogTCPSummary(stats) {
		logf(LogLevelInfo,
			"[INFO] [TCP] %s -> %s (%s:%s) id=%s | %s | up %s / down %s | %s",
			source, target, OutboundDirect, decision.Rule, flowID,
			formatDuration(stats.Duration),
			formatBytes(stats.UploadBytes), formatBytes(stats.DownloadBytes),
			combineResults(stats.UploadErr, stats.DownloadErr),
		)
	}
}

func (e *Engine) dialDirectTCP(ctx context.Context, target string, budget time.Duration) (net.Conn, error) {
	if budget <= 0 {
		budget = transientTCPDialBudgetCap
	}
	deadline := time.Now().Add(budget)
	var lastErr error

	for {
		wait := tcpDialAcquireTimeout
		if remaining := time.Until(deadline); remaining > 0 && remaining < wait {
			wait = remaining
		}
		if !e.acquireTCPDialSlot(ctx, wait) {
			if time.Now().Before(deadline) && e.sleepWithContext(ctx, tcpDialCooldownPollInterval) == nil {
				lastErr = errTCPDialThrottle
				continue
			}
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, errTCPDialThrottle
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			e.releaseTCPDialSlot()
			return nil, context.DeadlineExceeded
		}
		dialDeadlineCtx, cancelDialDeadline := context.WithDeadline(ctx, deadline)
		dialCtx, cancelDial, generation := e.bindDialContext(dialDeadlineCtx)
		dialer := &net.Dialer{Timeout: remaining, KeepAlive: 30 * time.Second}
		upConn, err := dialer.DialContext(dialCtx, "tcp", target)
		cancelDial()
		cancelDialDeadline()
		e.releaseTCPDialSlot()
		if err == nil && e.activateUpstreamForGeneration(upConn, generation) {
			return upConn, nil
		}
		if err == nil {
			err = context.Canceled
		}
		lastErr = err
		if !isTransientUpstreamDialError(err) || time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		if isAddrNotAvailableError(err) {
			e.markTCPDialBackoff(transientTCPDialRetryDelay)
		}
		if e.sleepWithContext(ctx, transientTCPDialRetryDelay) != nil {
			return nil, err
		}
	}
}

/* Bridges one UDP flow when the selected upstream supports it. */
func (e *Engine) handleUDP(ctx context.Context, id gstack.TransportEndpointID, localConn net.PacketConn) {
	defer localConn.Close()
	if !e.activateLocal(localConn) {
		return
	}
	defer e.unregisterActiveLocal(localConn)

	flowID := e.nextFlowID("udp")
	target := udpDestination(id)
	targetStr := udpDestinationString(id)
	sourceStr := udpSourceString(id)
	if target == nil || len(target.IP) == 0 || target.Port <= 0 {
		logf(LogLevelWarn, "[WARN] [UDP] [%s] dropped: invalid target | mode=%s %s -> %s", flowID, e.dialer.Mode(), sourceStr, targetStr)
		return
	}
	if shouldRelayDNSOverTCP(target) {
		e.handleDNSOverTCP(ctx, flowID, sourceStr, target, targetStr, localConn)
		return
	}
	if !e.cfg.Runtime.EnableUDP {
		logf(LogLevelDebug, "[DEBUG] [UDP] [%s] dropped: udp disabled | %s -> %s", flowID, sourceStr, targetStr)
		return
	}
	decision := e.routeDecisionForIP(target.IP)
	if decision.Outbound == OutboundDirect {
		if e.isUDPNoResponseCoolingDown(targetStr) {
			logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s:%s) id=%s | dropped: no-response cooldown", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID)
			return
		}
		if !e.acquireDirectUDPActive(targetStr) {
			logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s:%s) id=%s | dropped: no-response probe limit", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID)
			return
		}
		defer e.releaseDirectUDPActive(targetStr)
		if err := e.ensureDirectRoute(target.IP, decision); err != nil {
			logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s:%s) id=%s | route failed | err=%v", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID, err)
			return
		}
		e.handleDirectUDP(ctx, flowID, sourceStr, target, targetStr, localConn, decision)
		return
	}
	if e.isUDPNoResponseCoolingDown(targetStr) {
		logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | dropped: no-response cooldown", sourceStr, targetStr, e.dialer.Mode(), flowID)
		return
	}
	if shouldDropProxiedUDP(target) {
		e.markUDPNoResponseBackoff(targetStr, udpNoResponseBackoffDuration)
		e.logUDPPolicyDrop(flowID, e.dialer.Mode(), sourceStr, targetStr, "proxied udp/443 disabled for fast tcp fallback")
		return
	}
	poolKey := udpSessionPoolKey(sourceStr, targetStr)
	if !e.dialer.SupportsUDP() || e.cfg.Mode == ModeHTTPS {
		e.markUDPNoResponseBackoff(targetStr, udpNoResponseBackoffDuration)
		e.logUDPPolicyDrop(flowID, e.dialer.Mode(), sourceStr, targetStr, "policy restriction")
		return
	}

	relayGeneration := e.networkGeneration.Load()
	session := e.takeIdleUDPSession(poolKey)
	if session == nil {
		if e.isUDPDialCoolingDown() {
			logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | dropped: dial cooldown", sourceStr, targetStr, e.dialer.Mode(), flowID)
			return
		}
		if !e.acquireUDPDialSlot(ctx, udpDialAcquireTimeout) {
			logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | dropped: dial throttle", sourceStr, targetStr, e.dialer.Mode(), flowID)
			return
		}

		dialCtx, cancelDial, generation := e.bindDialContext(ctx)
		relayGeneration = generation
		var err error
		session, err = e.dialer.DialUDP(dialCtx)
		cancelDial()
		e.releaseUDPDialSlot()
		if err != nil {
			if isAddrNotAvailableError(err) {
				e.markUDPDialBackoffForGeneration(generation, 500*time.Millisecond)
			}
			logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s) id=%s | dial failed | err=%s", sourceStr, targetStr, e.dialer.Mode(), flowID, formatLogError(err))
			return
		}
		if !e.activateUpstreamForGeneration(session, generation) {
			return
		}
	}
	reusableSession := false
	defer func() {
		e.releaseUDPSession(poolKey, session, reusableSession)
	}()

	logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | relay start", sourceStr, targetStr, e.dialer.Mode(), flowID)

	idleTimeout := e.cfg.UDPIdleTimeout()
	resultCh := make(chan udpPumpResult, 2)
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			_ = session.SetDeadline(time.Now())
			_ = localConn.Close()
		})
	}
	idleWatchdog := newIdleWatchdog(idleTimeout)
	idleDone := make(chan struct{})
	idleExpired := make(chan struct{})
	go idleWatchdog.run(ctx.Done(), idleDone, func() {
		closeAll()
		close(idleExpired)
	})

	udpBufSize := e.cfg.Runtime.UDPBuffer
	go func() {
		res := udpPumpResult{Direction: "local_to_upstream"}
		defer func() { resultCh <- res }()
		pooledBuf := getPooledBuf(&e.relayBufPool, udpBufSize)
		defer putPooledBuf(&e.relayBufPool, pooledBuf)
		buf := *pooledBuf
		for {
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return
			}
			n, _, err := localConn.ReadFrom(buf)
			if n > 0 {
				res.Bytes += int64(n)
				res.Packets++
				if wErr := session.WriteTo(buf[:n], target); wErr != nil {
					logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s) id=%s | write upstream failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, wErr)
					res.Err = wErr
					return
				}
				idleWatchdog.touch()
			}
			if err != nil {
				if isTimeoutError(err) {
					logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | idle timeout (local->upstream)", sourceStr, targetStr, e.dialer.Mode(), flowID)
					res.Err = err
					return
				}
				if isLocalPacketReadClosedError(err) {
					logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | local reader closed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
				} else if !isClosedConnError(err) {
					logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s) id=%s | read local failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
				}
				res.Err = err
				return
			}
		}
	}()

	go func() {
		res := udpPumpResult{Direction: "upstream_to_local"}
		defer func() { resultCh <- res }()
		pooledBuf := getPooledBuf(&e.relayBufPool, udpBufSize)
		defer putPooledBuf(&e.relayBufPool, pooledBuf)
		buf := *pooledBuf
		for {
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return
			}
			n, src, err := session.ReadFrom(buf)
			if n > 0 {
				if !sameUDPEndpoint(src, target) {
					logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | dropped: mismatched upstream source=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, src)
					continue
				}
				res.Bytes += int64(n)
				res.Packets++
				if _, wErr := writeBackToPacketConn(localConn, buf[:n]); wErr != nil {
					if isBenignLocalPacketWriteError(wErr) {
						logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | local receiver closed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, wErr)
					} else {
						logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s) id=%s | write local failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, wErr)
					}
					res.Err = wErr
					res.LocalWriteErr = true
					return
				}
				idleWatchdog.touch()
			}
			if err != nil {
				if isTimeoutError(err) {
					logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s) id=%s | idle timeout (upstream->local)", sourceStr, targetStr, e.dialer.Mode(), flowID)
					res.Err = err
					return
				}
				if !isClosedConnError(err) {
					logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s) id=%s | read upstream failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
				}
				res.Err = err
				return
			}
		}
	}()

	start := time.Now()
	completed := 0
	localToUpstream := udpPumpResult{Direction: "local_to_upstream"}
	upstreamToLocal := udpPumpResult{Direction: "upstream_to_local"}
	select {
	case <-ctx.Done():
	case res := <-resultCh:
		if res.Direction == "local_to_upstream" {
			localToUpstream = res
		} else {
			upstreamToLocal = res
		}
		completed = 1
	case <-idleExpired:
	}
	close(idleDone)
	closeAll()
	warningAfter := udpPumpDrainWarningTimeout
	forceWait := udpPumpDrainForceTimeout
	forceClose := func() {
		_ = session.Close()
		_ = localConn.Close()
	}
	warn := func() {
		logf(LogLevelWarn, "[WARN] [UDP] %s -> %s (%s) id=%s | pumps still draining after 2s; force closing endpoints", sourceStr, targetStr, e.dialer.Mode(), flowID)
	}
	var drained bool
	localToUpstream, upstreamToLocal, drained = drainUDPPumpResults(
		resultCh, completed, localToUpstream, upstreamToLocal,
		warningAfter, forceWait, forceClose, warn,
	)
	duration := time.Since(start)
	/* Normalizes benign local write errors before deciding whether to reuse the UDP session. */
	u2lErrForReuse := upstreamToLocal.Err
	if upstreamToLocal.LocalWriteErr {
		u2lErrForReuse = nil
	}
	reusableSession = drained && e.shouldReuseUDPSession(ctx, localToUpstream.Err, u2lErrForReuse)
	if reusableSession && udpNoUpstreamResponse(localToUpstream, upstreamToLocal) {
		/* Rejects session reuse after the relay accepted outbound UDP but returned no usable packets. */
		reusableSession = false
	}
	if udpNoUpstreamResponse(localToUpstream, upstreamToLocal) {
		e.markUDPNoResponseBackoffForGeneration(targetStr, relayGeneration, udpNoResponseBackoffDuration)
	}
	if shouldLogUDPSummary(localToUpstream, upstreamToLocal, duration) {
		logf(LogLevelInfo,
			"[INFO] [UDP] %s -> %s (%s) id=%s | %s | up %s (%dp) / down %s (%dp) | %s",
			sourceStr, targetStr, e.dialer.Mode(), flowID,
			formatDuration(duration),
			formatBytes(localToUpstream.Bytes), localToUpstream.Packets,
			formatBytes(upstreamToLocal.Bytes), upstreamToLocal.Packets,
			combineUDPResults(localToUpstream, upstreamToLocal),
		)
	}
}

func (e *Engine) handleDirectUDP(ctx context.Context, flowID, sourceStr string, target *net.UDPAddr, targetStr string, localConn net.PacketConn, decision RouteDecision) {
	generation := e.networkGeneration.Load()
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		logf(LogLevelError, "[ERROR] [UDP] %s -> %s (%s:%s) id=%s | dial failed | err=%v", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID, err)
		return
	}
	if !e.activateUpstreamForGeneration(conn, generation) {
		return
	}
	defer e.unregisterActiveUpstream(conn)
	defer conn.Close()

	logf(LogLevelDebug, "[DEBUG] [UDP] %s -> %s (%s:%s) id=%s | relay start", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID)
	idleTimeout := e.cfg.UDPIdleTimeout()
	resultCh := make(chan udpPumpResult, 2)
	var closeOnce sync.Once
	closeAll := func() {
		closeOnce.Do(func() {
			_ = conn.SetDeadline(time.Now())
			_ = localConn.Close()
		})
	}
	idleWatchdog := newIdleWatchdog(idleTimeout)
	idleDone := make(chan struct{})
	idleExpired := make(chan struct{})
	go idleWatchdog.run(ctx.Done(), idleDone, func() {
		closeAll()
		close(idleExpired)
	})

	udpBufSize := e.cfg.Runtime.UDPBuffer
	go func() {
		res := udpPumpResult{Direction: "local_to_upstream"}
		defer func() { resultCh <- res }()
		pooledBuf := getPooledBuf(&e.relayBufPool, udpBufSize)
		defer putPooledBuf(&e.relayBufPool, pooledBuf)
		buf := *pooledBuf
		for {
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return
			}
			n, _, err := localConn.ReadFrom(buf)
			if n > 0 {
				res.Bytes += int64(n)
				res.Packets++
				if _, wErr := conn.WriteToUDP(buf[:n], target); wErr != nil {
					res.Err = wErr
					return
				}
				idleWatchdog.touch()
			}
			if err != nil {
				res.Err = err
				return
			}
		}
	}()

	go func() {
		res := udpPumpResult{Direction: "upstream_to_local"}
		defer func() { resultCh <- res }()
		pooledBuf := getPooledBuf(&e.relayBufPool, udpBufSize)
		defer putPooledBuf(&e.relayBufPool, pooledBuf)
		buf := *pooledBuf
		for {
			if ctx.Err() != nil {
				res.Err = ctx.Err()
				return
			}
			n, src, err := conn.ReadFromUDP(buf)
			if n > 0 {
				if !sameUDPEndpoint(src, target) {
					continue
				}
				res.Bytes += int64(n)
				res.Packets++
				if _, wErr := writeBackToPacketConn(localConn, buf[:n]); wErr != nil {
					res.Err = wErr
					res.LocalWriteErr = true
					return
				}
				idleWatchdog.touch()
			}
			if err != nil {
				res.Err = err
				return
			}
		}
	}()

	start := time.Now()
	localToUpstream := udpPumpResult{Direction: "local_to_upstream"}
	upstreamToLocal := udpPumpResult{Direction: "upstream_to_local"}
	completed := 0
	select {
	case <-ctx.Done():
	case res := <-resultCh:
		if res.Direction == "local_to_upstream" {
			localToUpstream = res
		} else {
			upstreamToLocal = res
		}
		completed = 1
	case <-idleExpired:
	}
	close(idleDone)
	closeAll()
	warningAfter := udpPumpDrainWarningTimeout
	forceWait := udpPumpDrainForceTimeout
	forceClose := func() {
		_ = conn.Close()
		_ = localConn.Close()
	}
	warn := func() {
		logf(LogLevelWarn, "[WARN] [UDP] %s -> %s (%s:%s) id=%s | pumps still draining after 2s; force closing endpoints", sourceStr, targetStr, OutboundDirect, decision.Rule, flowID)
	}
	localToUpstream, upstreamToLocal, _ = drainUDPPumpResults(
		resultCh, completed, localToUpstream, upstreamToLocal,
		warningAfter, forceWait, forceClose, warn,
	)
	duration := time.Since(start)
	if udpNoUpstreamResponse(localToUpstream, upstreamToLocal) {
		e.markUDPNoResponseBackoffForGeneration(targetStr, generation, udpNoResponseBackoffDuration)
	}
	if shouldLogUDPSummary(localToUpstream, upstreamToLocal, duration) {
		e.logDirectUDPSummary(flowID, sourceStr, targetStr, decision.Rule, duration, localToUpstream, upstreamToLocal)
	}
}

/* Relays DNS over a transient proxied TCP connection instead of UDP associate. */
func (e *Engine) handleDNSOverTCP(ctx context.Context, flowID, sourceStr string, target *net.UDPAddr, targetStr string, localConn net.PacketConn) {
	logf(LogLevelDebug, "[DEBUG] [DNS] %s -> %s (%s) id=%s | relay start (upstream=tcp)", sourceStr, targetStr, e.dialer.Mode(), flowID)

	idleTimeout := e.cfg.DNSExchangeTimeout()
	pooledBuf := getPooledBuf(&e.relayBufPool, e.cfg.Runtime.UDPBuffer)
	defer putPooledBuf(&e.relayBufPool, pooledBuf)
	buf := *pooledBuf
	upstream := udpPumpResult{Direction: "local_to_upstream"}
	downstream := udpPumpResult{Direction: "upstream_to_local"}
	start := time.Now()
	hadSuccess := false

	for {
		if ctx.Err() != nil {
			if upstream.Err == nil {
				upstream.Err = ctx.Err()
			}
			if downstream.Err == nil {
				downstream.Err = ctx.Err()
			}
			break
		}

		_ = localConn.SetReadDeadline(time.Now().Add(idleTimeout))
		n, _, err := localConn.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if isTimeoutError(err) {
				if hadSuccess {
					upstream.Err = nil
					downstream.Err = nil
					break
				}
				upstream.Err = err
				downstream.Err = err
				break
			}
			if !isClosedConnError(err) {
				logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | read local failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
			}
			upstream.Err = err
			downstream.Err = err
			break
		}
		if n == 0 {
			continue
		}

		payload := buf[:n]
		upstream.Bytes += int64(len(payload))
		upstream.Packets++

		if cachedResp := e.getDNSCache(targetStr, payload); cachedResp != nil {
			downstream.Bytes += int64(len(cachedResp))
			downstream.Packets++
			_ = localConn.SetWriteDeadline(time.Now().Add(idleTimeout))
			if _, err := writeBackToPacketConn(localConn, cachedResp); err != nil {
				downstream.Err = err
				if isBenignLocalPacketWriteError(err) {
					logf(LogLevelInfo, "[INFO] [DNS] %s -> %s (%s) id=%s | local receiver closed (cached) | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
				} else {
					logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | write cached local failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
				}
				break
			}
			hadSuccess = true
			upstream.Err = nil
			downstream.Err = nil
			continue
		}

		response, releaseResponse, dnsErr := e.exchangeDNSOverTCP(ctx, targetStr, payload, idleTimeout)
		if dnsErr != nil {
			if upstream.Err == nil {
				upstream.Err = dnsErr
			}
			if downstream.Err == nil {
				downstream.Err = dnsErr
			}
			e.logDNSExchangeFailure(flowID, e.dialer.Mode(), sourceStr, targetStr, dnsErr)
			break
		}

		e.putDNSCache(targetStr, payload, response)

		downstream.Bytes += int64(len(response))
		downstream.Packets++
		_ = localConn.SetWriteDeadline(time.Now().Add(idleTimeout))
		if _, err := writeBackToPacketConn(localConn, response); err != nil {
			releaseResponse()
			downstream.Err = err
			if isBenignLocalPacketWriteError(err) {
				logf(LogLevelInfo, "[INFO] [DNS] %s -> %s (%s) id=%s | local receiver closed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
			} else {
				logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | write local failed | err=%v", sourceStr, targetStr, e.dialer.Mode(), flowID, err)
			}
			break
		}
		releaseResponse()
		hadSuccess = true
		upstream.Err = nil
		downstream.Err = nil
		continue
	}

	duration := time.Since(start)
	if shouldLogUDPSummary(upstream, downstream, duration) {
		summaryUpErr, summaryDownErr := dnsSummaryErrors(upstream.Err, downstream.Err)
		logf(LogLevelInfo,
			"[INFO] [DNS] %s -> %s (%s) id=%s | %s | up %s (%dp) / down %s (%dp) | %s",
			sourceStr, targetStr, e.dialer.Mode(), flowID,
			formatDuration(duration),
			formatBytes(upstream.Bytes), upstream.Packets,
			formatBytes(downstream.Bytes), downstream.Packets,
			combineResults(summaryUpErr, summaryDownErr),
		)
	}
}

/* Performs one DNS-over-TCP request-response exchange through the configured upstream proxy. */
func (e *Engine) exchangeDNSOverTCP(ctx context.Context, targetAddr string, payload []byte, timeout time.Duration) ([]byte, func(), error) {
	if len(payload) > 65535 {
		return nil, func() {}, fmt.Errorf("dns tcp payload too large: %d bytes exceeds 65535-byte framing limit", len(payload))
	}
	var lastErr error
	forceFreshDial := false
	exchangeBudget := e.dnsConnectBudget(timeout)
	deadline := time.Now().Add(exchangeBudget)
	for attempt := 0; attempt < 2; attempt++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, func() {}, context.DeadlineExceeded
		}
		var (
			upConn     net.Conn
			pooled     bool
			generation uint64
			err        error
		)
		if forceFreshDial {
			upConn, err = e.dialTCPUpstream(ctx, targetAddr, remaining)
			if err == nil {
				generation, err = e.activeUpstreamGeneration(upConn)
			}
		} else {
			upConn, pooled, generation, err = e.acquireDNSConn(ctx, targetAddr, remaining)
		}
		if err != nil {
			if attempt == 0 && isTransientUpstreamDialError(err) {
				lastErr = err
				continue
			}
			return nil, func() {}, err
		}
		reuseConn := false /* connection persists in pool if exchange succeeds */

		response, releaseResponse, err := func() ([]byte, func(), error) {
			defer func() {
				if reuseConn {
					e.releaseDNSConn(targetAddr, upConn, generation)
					return
				}
				e.discardDNSConn(upConn)
			}()

			if err := upConn.SetDeadline(deadline); err != nil {
				return nil, func() {}, err
			}

			pooledFrame := getPooledBuf(&e.relayBufPool, 2+len(payload))
			defer putPooledBuf(&e.relayBufPool, pooledFrame)
			frame := (*pooledFrame)[:2+len(payload)]
			binary.BigEndian.PutUint16(frame[:2], uint16(len(payload)))
			copy(frame[2:], payload)
			if err := writeAll(upConn, frame); err != nil {
				return nil, func() {}, err
			}

			var lengthBuf [2]byte
			if _, err := io.ReadFull(upConn, lengthBuf[:]); err != nil {
				return nil, func() {}, err
			}
			size := int(binary.BigEndian.Uint16(lengthBuf[:]))
			if size == 0 {
				return nil, func() {}, errors.New("empty dns tcp response")
			}
			pooledResponse := getPooledBuf(&e.relayBufPool, size)
			response := (*pooledResponse)[:size]
			if _, err := io.ReadFull(upConn, response); err != nil {
				putPooledBuf(&e.relayBufPool, pooledResponse)
				return nil, func() {}, err
			}
			reuseConn = true
			return response, func() { putPooledBuf(&e.relayBufPool, pooledResponse) }, nil
		}()
		if err == nil {
			e.clearTCPTargetBackoffForGeneration(targetAddr, generation)
			return response, releaseResponse, nil
		}
		lastErr = err
		if pooled {
			forceFreshDial = true
		}
		if attempt == 0 && (pooled || isRetryableDNSExchangeError(err)) {
			continue
		}
		return nil, func() {}, err
	}
	return nil, func() {}, lastErr
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "use of closed network connection") ||
		strings.Contains(msg, "closed pipe")
}

func isLocalPacketReadClosedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || isClosedConnError(err)
}

/* Reports whether the error is a benign local packet write failure. */
func isBenignLocalPacketWriteError(err error) bool {
	if err == nil {
		return false
	}
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) {
		return sysErr == syscall.ECONNREFUSED
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection was refused") ||
		strings.Contains(msg, "port unreachable")
}

func isRetryableDNSExchangeError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "reset by peer") ||
		strings.Contains(msg, "use of closed network connection")
}

func (e *Engine) logDNSExchangeFailure(flowID, mode, source, target string, err error) {
	if isContextCanceledLike(err) {
		logf(LogLevelDebug, "[DEBUG] [DNS] %s -> %s (%s) id=%s | exchange canceled", source, target, mode, flowID)
		return
	}
	if errors.Is(err, errTCPTargetCooldown) || errors.Is(err, errTCPTargetThrottle) {
		reason := "target dial limit"
		if errors.Is(err, errTCPTargetCooldown) {
			reason = "target cooldown"
		}
		e.logTCPTargetPressure(flowID, mode, source, target, reason)
		return
	}
	if err == nil || (!isRetryableDNSExchangeError(err) && !errors.Is(err, errTCPDialCooldown) && !isTransientUpstreamDialError(err)) {
		logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | exchange failed | err=%s", source, target, mode, flowID, formatLogError(err))
		return
	}
	/* Groups failures by mode, target, and error class to prevent log flooding. */
	key := mode + "|dns|" + target + "|" + dnsFailureClass(err)
	now := time.Now()

	e.logThrottleMu.Lock()
	if e.logThrottleStates == nil {
		e.logThrottleStates = make(map[string]logThrottleState)
	}
	if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.pruneLogThrottleStatesLocked(now)
	}
	if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.logThrottleMu.Unlock()
		logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | exchange failed | err=%s", source, target, mode, flowID, formatLogError(err))
		return
	}
	state := e.logThrottleStates[key]
	if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
		state.Suppressed++
		e.logThrottleStates[key] = state
		e.logThrottleMu.Unlock()
		return
	}
	repeats := state.Suppressed
	state.LastLogTime = now
	state.Suppressed = 0
	e.logThrottleStates[key] = state
	e.logThrottleMu.Unlock()

	if repeats > 0 {
		logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | exchange failed | err=%s | repeats=%d", source, target, mode, flowID, formatLogError(err), repeats)
		return
	}
	logf(LogLevelError, "[ERROR] [DNS] %s -> %s (%s) id=%s | exchange failed | err=%s", source, target, mode, flowID, formatLogError(err))
}

func dnsFailureClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	case isContextCanceledLike(err):
		return "canceled"
	case errors.Is(err, errTCPDialCooldown):
		return "cooldown"
	case errors.Is(err, errTCPTargetCooldown):
		return "target-cooldown"
	case errors.Is(err, errTCPTargetThrottle):
		return "target-throttle"
	case isAddrNotAvailableError(err):
		return "eaddrnotavail"
	case isTransientUpstreamDialError(err):
		return "transient"
	case isTimeoutError(err):
		return "timeout"
	case isRetryableDNSExchangeError(err):
		return "retryable"
	default:
		return "other"
	}
}

func writeBackToPacketConn(conn net.PacketConn, payload []byte) (int, error) {
	if writer, ok := conn.(interface{ Write([]byte) (int, error) }); ok {
		return writer.Write(payload)
	}
	return 0, fmt.Errorf("packet conn %T does not support connected writes", conn)
}

/* Checks whether the target port matches a known DNS port. */
func shouldRelayDNSOverTCP(target *net.UDPAddr) bool {
	return target != nil && target.Port == 53
}

func isUpstreamLoopTarget(target, upstream string) bool {
	tHost, tPort, err := net.SplitHostPort(strings.TrimSpace(target))
	if err != nil {
		return false
	}
	uHost, uPort, err := net.SplitHostPort(strings.TrimSpace(upstream))
	if err != nil {
		return false
	}
	if tPort != uPort {
		return false
	}

	tIP := net.ParseIP(tHost)
	uIP := net.ParseIP(uHost)
	if tIP != nil && uIP != nil {
		return tIP.Equal(uIP)
	}
	return strings.EqualFold(strings.TrimSpace(tHost), strings.TrimSpace(uHost))
}

func isAnyUpstreamLoopTarget(target string, upstreams []UpstreamConfig) bool {
	for _, upstream := range upstreams {
		if isUpstreamLoopTarget(target, upstream.Addr) {
			return true
		}
	}
	return false
}

type udpPumpResult struct {
	Direction     string
	Bytes         int64
	Packets       int64
	Err           error
	LocalWriteErr bool /* true when Err came from writing back to the local TUN socket (not from reading upstream) */
}

/* Waits for both pumps, then force-closes and gives stuck implementations one final bounded drain window. */
func drainUDPPumpResults(
	resultCh <-chan udpPumpResult,
	completed int,
	localToUpstream, upstreamToLocal udpPumpResult,
	warningAfter, forceWait time.Duration,
	forceClose, warn func(),
) (udpPumpResult, udpPumpResult, bool) {
	if warningAfter <= 0 {
		warningAfter = time.Nanosecond
	}
	if forceWait <= 0 {
		forceWait = time.Nanosecond
	}
	warningTimer := time.NewTimer(warningAfter)
	defer warningTimer.Stop()
	warningC := warningTimer.C
	var forceTimer *time.Timer
	var forceC <-chan time.Time
	defer func() {
		if forceTimer != nil {
			forceTimer.Stop()
		}
	}()

	storeResult := func(result udpPumpResult) {
		if result.Direction == "local_to_upstream" {
			localToUpstream = result
			return
		}
		upstreamToLocal = result
	}
	for completed < 2 {
		select {
		case result := <-resultCh:
			storeResult(result)
			completed++
		case <-warningC:
			warningC = nil
			if warn != nil {
				warn()
			}
			if forceClose != nil {
				forceClose()
			}
			forceTimer = time.NewTimer(forceWait)
			forceC = forceTimer.C
		case <-forceC:
			return localToUpstream, upstreamToLocal, false
		}
	}
	return localToUpstream, upstreamToLocal, true
}

type relayCopyResult struct {
	Bytes int64
	Err   error
}

type bidirectionalRelayStats struct {
	UploadBytes   int64
	DownloadBytes int64
	UploadErr     error
	DownloadErr   error
	Duration      time.Duration
}

/* Copies bytes both ways and times out only when the whole relay is idle. */
func relayBidirectional(left, right net.Conn, bufSize int, idleTimeout time.Duration) {
	var pool relayBufferPool
	_ = relayBidirectionalWithStatsContext(context.Background(), left, right, bufSize, idleTimeout, &pool)
}

func relayBidirectionalWithStatsContext(ctx context.Context, left, right net.Conn, bufSize int, idleTimeout time.Duration, pool *relayBufferPool) bidirectionalRelayStats {
	start := time.Now()
	if bufSize <= 0 {
		bufSize = 64 * 1024
	}

	var wg sync.WaitGroup
	wg.Add(2)
	uploadCh := make(chan relayCopyResult, 1)
	downloadCh := make(chan relayCopyResult, 1)
	var interruptOnce sync.Once
	interruptRelay := func() {
		interruptOnce.Do(func() {
			now := time.Now()
			_ = left.SetDeadline(now)
			_ = right.SetDeadline(now)
			halfCloseRead(left)
			halfCloseWrite(left)
			halfCloseRead(right)
			halfCloseWrite(right)
			_ = left.Close()
			_ = right.Close()
		})
	}
	idleTracker := newIdleWatchdog(idleTimeout)
	relayDone := make(chan struct{})
	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}
	go idleTracker.run(ctxDone, relayDone, interruptRelay)

	go func() {
		defer wg.Done()
		pooledBuf := getPooledBuf(pool, bufSize)
		n, err := copyWithIdleWatchdog(right, left, *pooledBuf, idleTracker)
		putPooledBuf(pool, pooledBuf)
		uploadCh <- relayCopyResult{Bytes: n, Err: err}
		halfCloseWrite(right)
		halfCloseRead(left)
	}()

	go func() {
		defer wg.Done()
		pooledBuf := getPooledBuf(pool, bufSize)
		n, err := copyWithIdleWatchdog(left, right, *pooledBuf, idleTracker)
		putPooledBuf(pool, pooledBuf)
		downloadCh <- relayCopyResult{Bytes: n, Err: err}
		halfCloseWrite(left)
		halfCloseRead(right)
	}()

	wg.Wait()
	close(relayDone)
	interruptRelay()
	upload := <-uploadCh
	download := <-downloadCh
	return bidirectionalRelayStats{
		UploadBytes:   upload.Bytes,
		DownloadBytes: download.Bytes,
		UploadErr:     upload.Err,
		DownloadErr:   download.Err,
		Duration:      time.Since(start),
	}
}

func copyWithIdleTimeout(dst, src net.Conn, buf []byte, idleTimeout time.Duration) (int64, error) {
	watchdog := newIdleWatchdog(idleTimeout)
	done := make(chan struct{})
	go watchdog.run(nil, done, func() {
		now := time.Now()
		_ = src.SetReadDeadline(now)
		_ = dst.SetWriteDeadline(now)
	})
	n, err := copyWithIdleWatchdog(dst, src, buf, watchdog)
	close(done)
	return n, err
}

type idleWatchdog struct {
	mu           sync.Mutex
	timeout      time.Duration
	lastActivity time.Time
	activity     chan struct{}
}

func newIdleWatchdog(timeout time.Duration) *idleWatchdog {
	return &idleWatchdog{
		timeout:      timeout,
		lastActivity: time.Now(),
		activity:     make(chan struct{}, 1),
	}
}

func (w *idleWatchdog) touch() {
	if w == nil || w.timeout <= 0 {
		return
	}
	w.mu.Lock()
	w.lastActivity = time.Now()
	w.mu.Unlock()
	select {
	case w.activity <- struct{}{}:
	default:
	}
}

func (w *idleWatchdog) remaining() time.Duration {
	w.mu.Lock()
	lastActivity := w.lastActivity
	w.mu.Unlock()
	return w.timeout - time.Since(lastActivity)
}

func (w *idleWatchdog) run(ctxDone, done <-chan struct{}, expire func()) {
	if w == nil {
		return
	}
	var timer *time.Timer
	var timerC <-chan time.Time
	if w.timeout > 0 {
		timer = time.NewTimer(w.timeout)
		timerC = timer.C
		defer timer.Stop()
	}
	for {
		select {
		case <-ctxDone:
			if expire != nil {
				expire()
			}
			return
		case <-done:
			return
		case <-w.activity:
			if timer != nil {
				resetIdleTimer(timer, max(w.remaining(), time.Nanosecond))
			}
		case <-timerC:
			remaining := w.remaining()
			if remaining > 0 {
				resetIdleTimer(timer, remaining)
				continue
			}
			if expire != nil {
				expire()
			}
			return
		}
	}
}

func resetIdleTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func copyWithIdleWatchdog(dst, src net.Conn, buf []byte, watchdog *idleWatchdog) (int64, error) {
	var copied int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if err := writeAll(dst, buf[:n]); err != nil {
				return copied, err
			}
			copied += int64(n)
			watchdog.touch()
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return copied, nil
			}
			return copied, readErr
		}
	}
}

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func (e *Engine) acquireUDPDialSlot(ctx context.Context, wait time.Duration) bool {
	if e.udpDialSem == nil {
		return true
	}
	if wait <= 0 {
		wait = udpDialAcquireTimeout
	}
	slotCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	select {
	case e.udpDialSem <- struct{}{}:
		return true
	case <-slotCtx.Done():
		return false
	}
}

func (e *Engine) acquireTCPDialSlot(ctx context.Context, wait time.Duration) bool {
	if e.tcpDialSem == nil {
		return true
	}
	if wait <= 0 {
		wait = tcpDialAcquireTimeout
	}
	slotCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	select {
	case e.tcpDialSem <- struct{}{}:
		return true
	case <-slotCtx.Done():
		return false
	}
}

func (e *Engine) releaseUDPDialSlot() {
	if e.udpDialSem == nil {
		return
	}
	select {
	case <-e.udpDialSem:
	default:
	}
}

func (e *Engine) releaseTCPDialSlot() {
	if e.tcpDialSem == nil {
		return
	}
	select {
	case <-e.tcpDialSem:
	default:
	}
}

func (e *Engine) isUDPDialCoolingDown() bool {
	until := atomic.LoadInt64(&e.udpDialBackoffUntil)
	if until == 0 {
		return false
	}
	return time.Now().UnixNano() < until
}

func (e *Engine) isUDPNoResponseCoolingDown(target string) bool {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return false
	}
	now := time.Now().UnixNano()
	e.udpNoResponseMu.Lock()
	defer e.udpNoResponseMu.Unlock()
	if e.udpNoResponseBackoff == nil {
		return false
	}
	until := e.udpNoResponseBackoff[target]
	if until == 0 {
		return false
	}
	if now < until {
		return true
	}
	delete(e.udpNoResponseBackoff, target)
	return false
}

func (e *Engine) acquireDirectUDPActive(target string) bool {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return false
	}
	e.directUDPActiveMu.Lock()
	defer e.directUDPActiveMu.Unlock()
	if e.directUDPActive == nil {
		e.directUDPActive = make(map[string]int)
	}
	if e.directUDPActive[target] >= directUDPActiveTargetMax {
		return false
	}
	e.directUDPActive[target]++
	return true
}

func (e *Engine) releaseDirectUDPActive(target string) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return
	}
	e.directUDPActiveMu.Lock()
	defer e.directUDPActiveMu.Unlock()
	if e.directUDPActive == nil {
		return
	}
	count := e.directUDPActive[target]
	if count <= 1 {
		delete(e.directUDPActive, target)
		return
	}
	e.directUDPActive[target] = count - 1
}

func (e *Engine) isTCPDialCoolingDown() bool {
	until := atomic.LoadInt64(&e.tcpDialBackoffUntil)
	if until == 0 {
		return false
	}
	return time.Now().UnixNano() < until
}

func (e *Engine) isTCPTargetCoolingDown(target string) bool {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return false
	}
	now := time.Now().UnixNano()
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	until := e.tcpTargetBackoff[target]
	if until == 0 {
		return false
	}
	if now < until {
		return true
	}
	delete(e.tcpTargetBackoff, target)
	if e.tcpTargetActive[target] == 0 {
		delete(e.tcpTargetSuccess, target)
	}
	return false
}

type tcpTargetDialAttempt struct {
	target       string
	generation   uint64
	successEpoch uint64
}

func (e *Engine) beginTCPTargetDial(target string) (tcpTargetDialAttempt, error) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return tcpTargetDialAttempt{}, errTCPTargetThrottle
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()

	now := time.Now().UnixNano()
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	if until := e.tcpTargetBackoff[target]; until > now {
		return tcpTargetDialAttempt{}, errTCPTargetCooldown
	} else if until != 0 {
		delete(e.tcpTargetBackoff, target)
		if e.tcpTargetActive[target] == 0 {
			delete(e.tcpTargetSuccess, target)
		}
	}
	if e.tcpTargetActive == nil {
		e.tcpTargetActive = make(map[string]int)
	}
	if e.tcpTargetActive[target] >= tcpTargetDialConcurrency {
		return tcpTargetDialAttempt{}, errTCPTargetThrottle
	}
	attempt := tcpTargetDialAttempt{
		target:       target,
		generation:   e.networkGeneration.Load(),
		successEpoch: e.tcpTargetSuccess[target],
	}
	e.tcpTargetActive[target]++
	return attempt, nil
}

/* Lets a bounded number of bursts queue briefly behind the per-target dial cap. */
func (e *Engine) beginTCPTargetDialWithinBudget(ctx context.Context, target string, deadline time.Time) (tcpTargetDialAttempt, error) {
	attempt, err := e.beginTCPTargetDial(target)
	if err == nil || !errors.Is(err, errTCPTargetThrottle) {
		return attempt, err
	}
	if !e.acquireTCPTargetWaiter() {
		return tcpTargetDialAttempt{}, errTCPTargetThrottle
	}
	defer e.releaseTCPTargetWaiter()

	waitDeadline := time.Now().Add(tcpTargetDialWaitTimeout)
	if deadline.Before(waitDeadline) {
		waitDeadline = deadline
	}
	for {
		if time.Until(waitDeadline) <= 0 {
			return tcpTargetDialAttempt{}, errTCPTargetThrottle
		}

		wait := tcpDialCooldownPollInterval
		if remaining := time.Until(waitDeadline); remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return tcpTargetDialAttempt{}, ctx.Err()
		case <-timer.C:
		}
		attempt, err = e.beginTCPTargetDial(target)
		if err == nil || !errors.Is(err, errTCPTargetThrottle) {
			return attempt, err
		}
	}
}

func (e *Engine) acquireTCPTargetWaiter() bool {
	if e == nil {
		return false
	}
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	if e.tcpTargetWaiters >= tcpTargetDialWaitConcurrency {
		return false
	}
	e.tcpTargetWaiters++
	return true
}

func (e *Engine) releaseTCPTargetWaiter() {
	if e == nil {
		return
	}
	e.tcpTargetMu.Lock()
	if e.tcpTargetWaiters > 0 {
		e.tcpTargetWaiters--
	}
	e.tcpTargetMu.Unlock()
}

func (e *Engine) refreshTCPTargetDialAttempt(attempt tcpTargetDialAttempt, generation uint64) (tcpTargetDialAttempt, bool) {
	if e == nil || attempt.target == "" {
		return tcpTargetDialAttempt{}, false
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		return tcpTargetDialAttempt{}, false
	}
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	if e.tcpTargetActive[attempt.target] == 0 {
		return tcpTargetDialAttempt{}, false
	}
	attempt.generation = generation
	attempt.successEpoch = e.tcpTargetSuccess[attempt.target]
	return attempt, true
}

func (e *Engine) acquireTCPTargetDial(target string) bool {
	_, err := e.beginTCPTargetDial(target)
	return err == nil
}

func (e *Engine) releaseTCPTargetDial(target string) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return
	}
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	count := e.tcpTargetActive[target]
	if count <= 1 {
		delete(e.tcpTargetActive, target)
		if e.tcpTargetBackoff[target] == 0 {
			delete(e.tcpTargetSuccess, target)
		}
		return
	}
	e.tcpTargetActive[target] = count - 1
}

var (
	errTCPDialCooldown   = errors.New("tcp upstream dial cooldown")
	errTCPDialThrottle   = errors.New("tcp upstream dial throttle")
	errTCPTargetCooldown = errors.New("tcp target dial cooldown")
	errTCPTargetThrottle = errors.New("tcp target dial throttle")
)

func (e *Engine) markUDPDialBackoff(d time.Duration) {
	if d <= 0 {
		return
	}
	next := time.Now().Add(d).UnixNano()
	for {
		cur := atomic.LoadInt64(&e.udpDialBackoffUntil)
		if cur >= next {
			return
		}
		if atomic.CompareAndSwapInt64(&e.udpDialBackoffUntil, cur, next) {
			return
		}
	}
}

func (e *Engine) markUDPDialBackoffForGeneration(generation uint64, d time.Duration) {
	if e == nil {
		return
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		return
	}
	e.markUDPDialBackoff(d)
}

func (e *Engine) markUDPNoResponseBackoff(target string, d time.Duration) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" || d <= 0 {
		return
	}
	now := time.Now()
	until := now.Add(d).UnixNano()
	e.udpNoResponseMu.Lock()
	defer e.udpNoResponseMu.Unlock()
	if e.udpNoResponseBackoff == nil {
		e.udpNoResponseBackoff = make(map[string]int64)
	}
	if len(e.udpNoResponseBackoff) >= udpNoResponseBackoffMax {
		for key, value := range e.udpNoResponseBackoff {
			if value <= now.UnixNano() {
				delete(e.udpNoResponseBackoff, key)
			}
		}
	}
	if _, ok := e.udpNoResponseBackoff[target]; !ok && len(e.udpNoResponseBackoff) >= udpNoResponseBackoffMax {
		for key := range e.udpNoResponseBackoff {
			delete(e.udpNoResponseBackoff, key)
			break
		}
	}
	if e.udpNoResponseBackoff[target] < until {
		e.udpNoResponseBackoff[target] = until
	}
}

func (e *Engine) markUDPNoResponseBackoffForGeneration(target string, generation uint64, d time.Duration) {
	if e == nil {
		return
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		return
	}
	e.markUDPNoResponseBackoff(target, d)
}

func (e *Engine) markTCPDialBackoff(d time.Duration) {
	if d <= 0 {
		return
	}
	next := time.Now().Add(d).UnixNano()
	for {
		cur := atomic.LoadInt64(&e.tcpDialBackoffUntil)
		if cur >= next {
			return
		}
		if atomic.CompareAndSwapInt64(&e.tcpDialBackoffUntil, cur, next) {
			return
		}
	}
}

func (e *Engine) markTCPTargetBackoff(target string, d time.Duration) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" || d <= 0 {
		return
	}
	now := time.Now()
	until := now.Add(d).UnixNano()
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	e.markTCPTargetBackoffLocked(target, now.UnixNano(), until)
}

func (e *Engine) markTCPTargetBackoffForAttempt(attempt tcpTargetDialAttempt, d time.Duration) {
	if e == nil || attempt.target == "" || d <= 0 {
		return
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != attempt.generation {
		return
	}
	now := time.Now()
	until := now.Add(d).UnixNano()
	e.tcpTargetMu.Lock()
	defer e.tcpTargetMu.Unlock()
	if e.networkGeneration.Load() != attempt.generation || e.tcpTargetSuccess[attempt.target] != attempt.successEpoch {
		return
	}
	e.markTCPTargetBackoffLocked(attempt.target, now.UnixNano(), until)
}

func (e *Engine) markTCPTargetBackoffLocked(target string, now, until int64) {
	if e.tcpTargetBackoff == nil {
		e.tcpTargetBackoff = make(map[string]int64)
	}
	if len(e.tcpTargetBackoff) >= tcpTargetBackoffMax {
		for key, value := range e.tcpTargetBackoff {
			if value <= now {
				delete(e.tcpTargetBackoff, key)
				if e.tcpTargetActive[key] == 0 {
					delete(e.tcpTargetSuccess, key)
				}
			}
		}
	}
	if _, ok := e.tcpTargetBackoff[target]; !ok && len(e.tcpTargetBackoff) >= tcpTargetBackoffMax {
		for key := range e.tcpTargetBackoff {
			delete(e.tcpTargetBackoff, key)
			if e.tcpTargetActive[key] == 0 {
				delete(e.tcpTargetSuccess, key)
			}
			break
		}
	}
	if e.tcpTargetBackoff[target] < until {
		e.tcpTargetBackoff[target] = until
	}
}

func (e *Engine) clearTCPTargetBackoffForGeneration(target string, generation uint64) {
	target = strings.TrimSpace(target)
	if e == nil || target == "" {
		return
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		return
	}
	e.tcpTargetMu.Lock()
	if e.tcpTargetSuccess == nil {
		e.tcpTargetSuccess = make(map[string]uint64)
	}
	e.tcpTargetSuccess[target]++
	delete(e.tcpTargetBackoff, target)
	if e.tcpTargetActive[target] == 0 {
		delete(e.tcpTargetSuccess, target)
	}
	e.tcpTargetMu.Unlock()
}

func (e *Engine) clearTCPTargetBackoffForAttempt(attempt tcpTargetDialAttempt) {
	e.clearTCPTargetBackoffForGeneration(attempt.target, attempt.generation)
}

/* Clears transient dial cooldown so fresh routes can rebuild upstream sessions immediately. */
func (e *Engine) clearDialBackoff() {
	atomic.StoreInt64(&e.tcpDialBackoffUntil, 0)
	atomic.StoreInt64(&e.udpDialBackoffUntil, 0)
	e.udpNoResponseMu.Lock()
	clear(e.udpNoResponseBackoff)
	e.udpNoResponseMu.Unlock()
	e.tcpTargetMu.Lock()
	clear(e.tcpTargetBackoff)
	for target := range e.tcpTargetSuccess {
		if e.tcpTargetActive[target] == 0 {
			delete(e.tcpTargetSuccess, target)
		}
	}
	e.tcpTargetMu.Unlock()
}

func (e *Engine) pruneLogThrottleStatesLocked(now time.Time) {
	if len(e.logThrottleStates) == 0 {
		return
	}
	for key, state := range e.logThrottleStates {
		if state.LastLogTime.IsZero() || now.Sub(state.LastLogTime) > logThrottleStateTTL {
			delete(e.logThrottleStates, key)
		}
	}
}

/* Throttles logs for transient TCP dial failures to reduce IO overhead during network storms. */
func (e *Engine) logTCPDialFailure(flowID, mode, source, target string, err error) {
	if errors.Is(err, errTCPDialCooldown) || errors.Is(err, errTCPDialThrottle) ||
		errors.Is(err, errTCPTargetCooldown) || errors.Is(err, errTCPTargetThrottle) {
		/* Cooldown and throttle are expected pressure signals; callers already drop quietly. */
		return
	}

	key := mode + "|tcp-fail|" + target
	now := time.Now()

	e.logThrottleMu.Lock()
	if e.logThrottleStates == nil {
		e.logThrottleStates = make(map[string]logThrottleState)
	}
	if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.pruneLogThrottleStatesLocked(now)
	}
	if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.logThrottleMu.Unlock()
		logf(LogLevelError, "[ERROR] [TCP] %s -> %s (%s) id=%s | dial failed | err=%s", source, target, mode, flowID, formatLogError(err))
		return
	}
	state := e.logThrottleStates[key]
	if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
		state.Suppressed++
		e.logThrottleStates[key] = state
		e.logThrottleMu.Unlock()
		return
	}

	repeats := state.Suppressed
	state.LastLogTime = now
	state.Suppressed = 0
	e.logThrottleStates[key] = state
	e.logThrottleMu.Unlock()

	if repeats > 0 {
		logf(LogLevelError, "[ERROR] [TCP] %s -> %s (%s) id=%s | dial failed | err=%s | repeats=%d", source, target, mode, flowID, formatLogError(err), repeats)
		return
	}
	logf(LogLevelError, "[ERROR] [TCP] %s -> %s (%s) id=%s | dial failed | err=%s", source, target, mode, flowID, formatLogError(err))
}

/* Keeps target-pressure drops visible without restoring the original log storm. */
func (e *Engine) logTCPTargetPressure(flowID, mode, source, target, reason string) {
	key := mode + "|tcp-target-pressure|" + reason + "|" + target
	now := time.Now()

	e.logThrottleMu.Lock()
	if e.logThrottleStates == nil {
		e.logThrottleStates = make(map[string]logThrottleState)
	}
	if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.pruneLogThrottleStatesLocked(now)
	}
	if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.logThrottleMu.Unlock()
		logf(LogLevelInfo, "[INFO] [TCP] %s -> %s (%s) id=%s | dropped: %s", source, target, mode, flowID, reason)
		return
	}
	state := e.logThrottleStates[key]
	if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
		state.Suppressed++
		e.logThrottleStates[key] = state
		e.logThrottleMu.Unlock()
		return
	}

	repeats := state.Suppressed
	state.LastLogTime = now
	state.Suppressed = 0
	e.logThrottleStates[key] = state
	e.logThrottleMu.Unlock()

	if repeats > 0 {
		logf(LogLevelInfo, "[INFO] [TCP] %s -> %s (%s) id=%s | dropped: %s | repeats=%d", source, target, mode, flowID, reason, repeats)
		return
	}
	logf(LogLevelInfo, "[INFO] [TCP] %s -> %s (%s) id=%s | dropped: %s", source, target, mode, flowID, reason)
}

/* Throttles expected UDP policy drops so QUIC retry storms do not become log IO storms. */
func (e *Engine) logUDPPolicyDrop(flowID, mode, source, target, reason string) {
	key := mode + "|udp-policy|" + reason + "|" + target
	now := time.Now()

	e.logThrottleMu.Lock()
	if e.logThrottleStates == nil {
		e.logThrottleStates = make(map[string]logThrottleState)
	}
	if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.pruneLogThrottleStatesLocked(now)
	}
	if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.logThrottleMu.Unlock()
		logf(LogLevelDebug, "[DEBUG] [UDP] [%s] dropped: %s | %s -> %s", flowID, reason, source, target)
		return
	}
	state := e.logThrottleStates[key]
	if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
		state.Suppressed++
		e.logThrottleStates[key] = state
		e.logThrottleMu.Unlock()
		return
	}

	repeats := state.Suppressed
	state.LastLogTime = now
	state.Suppressed = 0
	e.logThrottleStates[key] = state
	e.logThrottleMu.Unlock()

	if repeats > 0 {
		logf(LogLevelDebug, "[DEBUG] [UDP] [%s] dropped: %s | %s -> %s | repeats=%d", flowID, reason, source, target, repeats)
		return
	}
	logf(LogLevelDebug, "[DEBUG] [UDP] [%s] dropped: %s | %s -> %s", flowID, reason, source, target)
}

/* Throttles UDP handler saturation logs so packet storms do not dominate logs. */
func (e *Engine) logUDPHandlerLimit(remote string) {
	key := "udp-handler-limit|" + remote
	now := time.Now()

	e.logThrottleMu.Lock()
	if e.logThrottleStates == nil {
		e.logThrottleStates = make(map[string]logThrottleState)
	}
	if len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.pruneLogThrottleStatesLocked(now)
	}
	if _, ok := e.logThrottleStates[key]; !ok && len(e.logThrottleStates) >= logThrottleStateMaxEntries {
		e.logThrottleMu.Unlock()
		logf(LogLevelSystem, "[SYS] [UDP] handler concurrency limit reached (%d); dropping flow from %s", cap(e.udpHandlerSem), remote)
		return
	}
	state := e.logThrottleStates[key]
	if !state.LastLogTime.IsZero() && now.Sub(state.LastLogTime) < logThrottleInterval {
		state.Suppressed++
		e.logThrottleStates[key] = state
		e.logThrottleMu.Unlock()
		return
	}

	repeats := state.Suppressed
	state.LastLogTime = now
	state.Suppressed = 0
	e.logThrottleStates[key] = state
	e.logThrottleMu.Unlock()

	if repeats > 0 {
		logf(LogLevelSystem, "[SYS] [UDP] handler concurrency limit reached (%d); dropping flow from %s | repeats=%d", cap(e.udpHandlerSem), remote, repeats)
		return
	}
	logf(LogLevelSystem, "[SYS] [UDP] handler concurrency limit reached (%d); dropping flow from %s", cap(e.udpHandlerSem), remote)
}

/* Derives a bounded retry window for transient upstream dial failures. */
func (e *Engine) transientTCPDialBudget() time.Duration {
	if e == nil || e.cfg == nil {
		return transientTCPDialBudgetCap
	}
	timeout := e.cfg.ConnectTimeout()
	switch {
	case timeout <= 0:
		return transientTCPDialBudgetCap
	case timeout < transientTCPDialBudgetCap:
		return timeout
	default:
		return transientTCPDialBudgetCap
	}
}

func (e *Engine) tcpConnectBudget() time.Duration {
	if e == nil || e.cfg == nil {
		return transientTCPDialBudgetCap
	}
	if timeout := e.cfg.ConnectTimeout(); timeout > 0 {
		return timeout
	}
	return transientTCPDialBudgetCap
}

func (e *Engine) dnsConnectBudget(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	if e == nil || e.cfg == nil {
		return transientTCPDialBudgetCap
	}
	if budget := e.cfg.DNSExchangeTimeout(); budget > 0 {
		return budget
	}
	return transientTCPDialBudgetCap
}

/* Dials the upstream TCP path with bounded retries for short route churn windows. */
func (e *Engine) dialTCPUpstream(ctx context.Context, target string, budget time.Duration) (net.Conn, error) {
	if budget <= 0 {
		budget = transientTCPDialBudgetCap
	}
	deadline := time.Now().Add(budget)
	targetAttempt, err := e.beginTCPTargetDialWithinBudget(ctx, target, deadline)
	if err != nil {
		return nil, err
	}
	defer e.releaseTCPTargetDial(target)

	var lastErr error

	for attempt := 0; ; attempt++ {
		if err := e.waitForTCPDialCooldown(ctx, deadline); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}

		wait := tcpDialAcquireTimeout
		if remaining := time.Until(deadline); remaining > 0 && remaining < wait {
			wait = remaining
		}
		if !e.acquireTCPDialSlot(ctx, wait) {
			if time.Now().Before(deadline) && e.sleepWithContext(ctx, tcpDialCooldownPollInterval) == nil {
				lastErr = errTCPDialThrottle
				continue
			}
			return nil, errTCPDialThrottle
		}

		var upConn net.Conn
		var err error
		if dialer, ok := e.dialer.(attemptBudgetTCPDialer); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				e.releaseTCPDialSlot()
				return nil, context.DeadlineExceeded
			}
			dialDeadlineCtx, cancelDialDeadline := context.WithDeadline(ctx, deadline)
			dialCtx, cancelDial, generation := e.bindDialContext(dialDeadlineCtx)
			if refreshed, current := e.refreshTCPTargetDialAttempt(targetAttempt, generation); current {
				targetAttempt = refreshed
			}
			upConn, err = dialer.DialTCPWithAttemptBudget(dialCtx, target, remaining)
			cancelDial()
			cancelDialDeadline()
			if err == nil && !e.activateUpstreamForGeneration(upConn, generation) {
				err = context.Canceled
			}
		} else {
			dialDeadlineCtx, cancelDialDeadline := context.WithDeadline(ctx, deadline)
			dialCtx, cancelDial, generation := e.bindDialContext(dialDeadlineCtx)
			if refreshed, current := e.refreshTCPTargetDialAttempt(targetAttempt, generation); current {
				targetAttempt = refreshed
			}
			upConn, err = e.dialer.DialTCP(dialCtx, target)
			cancelDial()
			cancelDialDeadline()
			if err == nil && !e.activateUpstreamForGeneration(upConn, generation) {
				err = context.Canceled
			}
		}
		e.releaseTCPDialSlot()
		if err == nil {
			e.clearTCPTargetBackoffForAttempt(targetAttempt)
			return upConn, nil
		}
		lastErr = err
		if ctx.Err() == nil && allTargetSpecificSOCKSReplyErrors(err) {
			e.markTCPTargetBackoffForAttempt(targetAttempt, tcpTargetBackoffDuration)
		}

		if !isTransientUpstreamDialError(err) || time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		if isAddrNotAvailableError(err) {
			e.markTCPDialBackoff(transientTCPDialRetryDelay)
		}
		if e.sleepWithContext(ctx, transientTCPDialRetryDelay) != nil {
			return nil, err
		}
	}
}

func (e *Engine) routeDecisionForTarget(target string) RouteDecision {
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return RouteDecision{Outbound: OutboundProxy, Rule: "invalid-target"}
	}
	return e.routeDecisionForIP(net.ParseIP(strings.TrimSpace(host)))
}

func (e *Engine) routeDecisionForIP(ip net.IP) RouteDecision {
	if e == nil || e.router == nil || !e.router.Enabled() {
		return RouteDecision{Outbound: OutboundProxy, Rule: "routing-disabled"}
	}
	return e.router.DecideIP(ip)
}

func (e *Engine) ensureDirectRouteForTarget(target string, decision RouteDecision) error {
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("direct route target: %w", err)
	}
	return e.ensureDirectRoute(net.ParseIP(strings.TrimSpace(host)), decision)
}

func (e *Engine) ensureDirectRoute(ip net.IP, decision RouteDecision) error {
	if decision.Outbound != OutboundDirect {
		return nil
	}
	ip = normalizeIP(ip)
	if ip == nil {
		return nil
	}
	e.routeHookMu.RLock()
	hook := e.onDirectRoute
	e.routeHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(DirectRoute{IP: append(net.IP(nil), ip...), Decision: decision})
}

/* Waits for a transient global tcp cooldown to clear before attempting a fresh dial. */
func (e *Engine) waitForTCPDialCooldown(ctx context.Context, deadline time.Time) error {
	for e.isTCPDialCoolingDown() {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return errTCPDialCooldown
		}
		if err := e.sleepWithContext(ctx, tcpDialCooldownPollInterval); err != nil {
			return err
		}
	}
	return nil
}

/* Sleeps while still honoring caller cancellation. */
func (e *Engine) sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

/* Classifies route-churn dial failures that merit a bounded retry. */
func isTransientUpstreamDialError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRNOTAVAIL) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "can't assign requested address") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe")
}

func isAddrNotAvailableError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.EADDRNOTAVAIL) || strings.Contains(strings.ToLower(err.Error()), "can't assign requested address")
}

func (e *Engine) nextFlowID(prefix string) string {
	n := atomic.AddUint64(&e.flowSeq, 1)
	return prefix + strconv.FormatUint(n, 10)
}

/* Aggressively tears down stale upstream state after the host default route changes. */
func (e *Engine) HandleNetworkChange() {
	e.upstreamLifecycleMu.Lock()
	e.resetDialContext()
	e.clearDialBackoff()
	/* Reset selection after advancing the dial generation. A dial that starts in
	   the narrow reset window is then either invalidated by this selection epoch
	   or belongs to the new network generation. */
	e.resetUpstreamSelection()
	e.closeIdleDNSPool()
	e.closeIdleUDPPool()
	e.closeActiveUpstreams()
	e.upstreamLifecycleMu.Unlock()
	e.clearLogThrottleStates()
	logf(LogLevelSystem, "[SYS] [NET] network change detected: reset upstream dials and closed active upstream sessions")
	e.warmupUpstreamTransport()
}

func (e *Engine) bindDialContext(parent context.Context) (context.Context, context.CancelFunc, uint64) {
	e.dialCtxMu.Lock()
	if e.dialCtx == nil || e.dialCancel == nil {
		e.dialCtx, e.dialCancel = context.WithCancel(context.Background())
	}
	routeCtx := e.dialCtx
	generation := e.networkGeneration.Load()
	e.dialCtxMu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(routeCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}, generation
}

func (e *Engine) resetDialContext() {
	e.dialCtxMu.Lock()
	defer e.dialCtxMu.Unlock()
	e.networkGeneration.Add(1)
	if e.dialCancel != nil {
		e.dialCancel()
	}
	e.dialCtx, e.dialCancel = context.WithCancel(context.Background())
}

type upstreamSelectionResetter interface {
	ResetUpstreamSelection()
}

func (e *Engine) resetUpstreamSelection() {
	if e == nil || e.dialer == nil {
		return
	}
	resetter, ok := e.dialer.(upstreamSelectionResetter)
	if !ok {
		return
	}
	resetter.ResetUpstreamSelection()
}

/* Opportunistically re-establishes the upstream transport after a route change. */
func (e *Engine) warmupUpstreamTransport() {
	if e == nil || e.dialer == nil || e.cfg == nil {
		return
	}
	warmer, ok := e.dialer.(UpstreamWarmer)
	if !ok {
		return
	}

	e.warmupMu.Lock()
	e.warmupPending = true
	if e.warmupRunning {
		e.warmupMu.Unlock()
		return
	}
	e.warmupRunning = true
	e.warmupMu.Unlock()

	timeout := e.cfg.ConnectTimeout()
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}

	go func(mode string) {
		for {
			e.warmupMu.Lock()
			if !e.warmupPending {
				e.warmupRunning = false
				e.warmupMu.Unlock()
				return
			}
			e.warmupPending = false
			e.warmupMu.Unlock()

			e.mu.Lock()
			stopping := e.stopping
			e.mu.Unlock()
			if stopping {
				e.warmupMu.Lock()
				e.warmupPending = false
				e.warmupRunning = false
				e.warmupMu.Unlock()
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			dialCtx, cancelDial, generation := e.bindDialContext(ctx)
			err := warmer.Warmup(dialCtx)
			cancelDial()
			ctxErr := ctx.Err()
			cancel()
			if err == nil {
				if e.networkGeneration.Load() == generation {
					logf(LogLevelSystem, "[SYS] upstream warmup succeeded | mode=%s", mode)
				}
				continue
			}
			if ctxErr == nil && !errors.Is(err, context.Canceled) && !isClosedConnError(err) {
				logf(LogLevelWarn, "[WARN] [SYS] upstream warmup failed | mode=%s err=%v", mode, err)
			}
		}
	}(e.dialer.Mode())
}

func (e *Engine) registerActiveUpstream(closer io.Closer) {
	if closer == nil {
		return
	}
	e.activeMu.Lock()
	if e.activeUpstreams == nil {
		e.activeUpstreams = make(map[io.Closer]struct{})
	}
	e.activeUpstreams[closer] = struct{}{}
	e.activeMu.Unlock()
}

func (e *Engine) unregisterActiveUpstream(closer io.Closer) {
	if closer == nil {
		return
	}
	e.activeMu.Lock()
	delete(e.activeUpstreams, closer)
	e.activeMu.Unlock()
}

func (e *Engine) unregisterTrackedUpstream(closer io.Closer) bool {
	if closer == nil {
		return false
	}
	e.activeMu.Lock()
	_, tracked := e.activeUpstreams[closer]
	if tracked {
		delete(e.activeUpstreams, closer)
	}
	e.activeMu.Unlock()
	return tracked
}

func (e *Engine) registerActiveLocal(closer io.Closer) {
	if closer == nil {
		return
	}
	e.activeMu.Lock()
	if e.activeLocals == nil {
		e.activeLocals = make(map[io.Closer]struct{})
	}
	e.activeLocals[closer] = struct{}{}
	e.activeMu.Unlock()
}

func (e *Engine) unregisterActiveLocal(closer io.Closer) {
	if closer == nil {
		return
	}
	e.activeMu.Lock()
	delete(e.activeLocals, closer)
	e.activeMu.Unlock()
}

func (e *Engine) closeActiveLocals() {
	e.activeMu.Lock()
	closers := make([]io.Closer, 0, len(e.activeLocals))
	for closer := range e.activeLocals {
		closers = append(closers, closer)
	}
	if len(closers) > 0 {
		clear(e.activeLocals)
	}
	e.activeMu.Unlock()

	now := time.Now()
	for _, closer := range closers {
		/* Unblocks pending reads and writes before closing. */
		if conn, ok := closer.(net.Conn); ok {
			_ = conn.SetDeadline(now)
		}
		_ = closer.Close()
	}
}

func (e *Engine) closeActiveUpstreams() {
	e.activeMu.Lock()
	closers := make([]io.Closer, 0, len(e.activeUpstreams))
	for closer := range e.activeUpstreams {
		closers = append(closers, closer)
	}
	if len(closers) > 0 {
		clear(e.activeUpstreams)
	}
	e.activeMu.Unlock()

	now := time.Now()
	for _, closer := range closers {
		/* Unblocks pending reads and writes before closing. */
		if conn, ok := closer.(net.Conn); ok {
			_ = conn.SetDeadline(now)
		}
		_ = closer.Close()
	}
}

func (e *Engine) acquireDNSConn(ctx context.Context, target string, budget time.Duration) (net.Conn, bool, uint64, error) {
	e.upstreamLifecycleMu.RLock()
	generation := e.networkGeneration.Load()
	e.dnsPoolMu.Lock()
	expired := e.pruneExpiredDNSPoolLocked(time.Now())
	var conn net.Conn
	if e.dnsPool != nil {
		if conns := e.dnsPool[target]; len(conns) > 0 {
			conn = conns[len(conns)-1].conn
			if len(conns) == 1 {
				delete(e.dnsPool, target)
			} else {
				e.dnsPool[target] = conns[:len(conns)-1]
			}
		}
	}
	e.dnsPoolMu.Unlock()
	for _, stale := range expired {
		_ = stale.Close()
	}
	if conn != nil {
		if !e.activateUpstream(conn) {
			e.upstreamLifecycleMu.RUnlock()
			return nil, false, 0, context.Canceled
		}
		e.upstreamLifecycleMu.RUnlock()
		return conn, true, generation, nil
	}
	e.upstreamLifecycleMu.RUnlock()

	conn, err := e.dialTCPUpstream(ctx, target, budget)
	if err != nil {
		return nil, false, 0, err
	}
	generation, err = e.activeUpstreamGeneration(conn)
	if err != nil {
		e.discardDNSConn(conn)
		return nil, false, 0, err
	}
	return conn, false, generation, nil
}

/* Returns a DNS connection to the idle pool or discards it if the pool is full. */
func (e *Engine) releaseDNSConn(target string, conn net.Conn, generation uint64) {
	if conn == nil {
		return
	}
	if err := clearConnDeadline(conn); err != nil {
		e.discardDNSConn(conn)
		return
	}

	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	if e.networkGeneration.Load() != generation {
		e.discardDNSConn(conn)
		return
	}
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	if stopping {
		e.discardDNSConn(conn)
		return
	}

	e.dnsPoolMu.Lock()
	if e.dnsPoolClosed {
		e.dnsPoolMu.Unlock()
		e.discardDNSConn(conn)
		return
	}
	if e.dnsPool == nil {
		e.dnsPool = make(map[string][]idleDNSConn)
	}
	expired := e.pruneExpiredDNSPoolLocked(time.Now())
	conns := e.dnsPool[target]
	if len(conns) >= dnsPoolPerTarget || e.dnsPoolSizeLocked() >= dnsPoolMaxIdle {
		e.dnsPoolMu.Unlock()
		for _, stale := range expired {
			_ = stale.Close()
		}
		e.discardDNSConn(conn)
		return
	}
	if !e.unregisterTrackedUpstream(conn) {
		e.dnsPoolMu.Unlock()
		for _, stale := range expired {
			_ = stale.Close()
		}
		_ = conn.Close()
		return
	}
	e.dnsPool[target] = append(conns, idleDNSConn{conn: conn, expiresAt: time.Now().Add(dnsPoolIdleTTL)})
	e.dnsPoolMu.Unlock()
	for _, stale := range expired {
		_ = stale.Close()
	}
}

func (e *Engine) activeUpstreamGeneration(conn net.Conn) (uint64, error) {
	if e == nil || conn == nil {
		return 0, context.Canceled
	}
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	generation := e.networkGeneration.Load()
	e.activeMu.Lock()
	_, active := e.activeUpstreams[conn]
	e.activeMu.Unlock()
	if !active {
		return 0, context.Canceled
	}
	return generation, nil
}

func (e *Engine) pruneExpiredDNSPoolLocked(now time.Time) []net.Conn {
	var expired []net.Conn
	for target, conns := range e.dnsPool {
		kept := conns[:0]
		for _, idle := range conns {
			if idle.conn == nil {
				continue
			}
			if !idle.expiresAt.IsZero() && !idle.expiresAt.After(now) {
				expired = append(expired, idle.conn)
				continue
			}
			kept = append(kept, idle)
		}
		if len(kept) == 0 {
			delete(e.dnsPool, target)
		} else {
			e.dnsPool[target] = kept
		}
	}
	return expired
}

func (e *Engine) dnsPoolSizeLocked() int {
	total := 0
	for _, conns := range e.dnsPool {
		total += len(conns)
	}
	return total
}

func (e *Engine) discardDNSConn(conn net.Conn) {
	if conn == nil {
		return
	}
	e.unregisterActiveUpstream(conn)
	_ = conn.Close()
}

func (e *Engine) getDNSCache(target string, payload []byte) []byte {
	if len(payload) < 12 {
		return nil
	}
	key := dnsCacheKey(target, payload)

	e.dnsCacheMu.RLock()
	entry, ok := e.dnsCache[key]
	e.dnsCacheMu.RUnlock()

	if !ok {
		return nil
	}
	if time.Now().After(entry.ExpiresAt) {
		e.dnsCacheMu.Lock()
		/* Avoids deleting an entry that was refreshed after the read-side expiry check. */
		if current, exists := e.dnsCache[key]; exists && time.Now().After(current.ExpiresAt) {
			e.deleteDNSCacheEntryLocked(key, current)
		}
		e.dnsCacheMu.Unlock()
		return nil
	}

	/* Reuses the cached response and restores the caller transaction ID. */
	logf(LogLevelDebug, "[DEBUG] [DNS] %s | cache hit", target)
	resp := append([]byte(nil), entry.Response...)
	if len(resp) >= 2 {
		resp[0] = payload[0]
		resp[1] = payload[1]
	}
	return resp
}

func (e *Engine) putDNSCache(target string, reqPayload, respPayload []byte) {
	if len(reqPayload) < 12 || len(respPayload) < 12 {
		return
	}
	reqQuestionKey := dnsQuestionIdentity(reqPayload)
	if reqQuestionKey == "" || reqQuestionKey != dnsQuestionIdentity(respPayload) {
		return /* Skips mismatched or malformed responses so they cannot poison the request-scoped cache. */
	}
	key := dnsCacheKey(target, reqPayload)
	entrySize := int64(len(key) + len(respPayload))
	if entrySize > e.dnsCacheByteLimit() {
		return
	}
	ttlSeconds := extractMinDNSTTL(respPayload)
	if ttlSeconds <= 0 {
		return /* Skips uncachable responses. */
	}
	if ttlSeconds > 3600 {
		ttlSeconds = 3600 /* Caps cached TTL at one hour. */
	}
	expiresAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	response := make([]byte, len(respPayload))
	copy(response, respPayload)

	e.dnsCacheMu.Lock()
	defer e.dnsCacheMu.Unlock()

	if e.dnsCache == nil {
		e.dnsCache = make(map[string]dnsCacheEntry)
	}
	if current, exists := e.dnsCache[key]; exists {
		e.deleteDNSCacheEntryLocked(key, current)
	}

	/* Performs bounded opportunistic expiry work before enforcing hard budgets. */
	maxDNSCacheSize := defaultDNSCacheSize
	if e.cfg != nil && e.cfg.Runtime.DNSCacheSize > 0 {
		maxDNSCacheSize = e.cfg.Runtime.DNSCacheSize
	}
	now := time.Now()
	scanned := 0
	for k, entry := range e.dnsCache {
		if now.After(entry.ExpiresAt) {
			e.deleteDNSCacheEntryLocked(k, entry)
		}
		scanned++
		if scanned >= dnsCacheExpirySweepEntries {
			break
		}
	}
	byteLimit := e.dnsCacheByteLimit()
	for len(e.dnsCache) >= maxDNSCacheSize || e.dnsCacheBytes+entrySize > byteLimit {
		evicted := false
		for k, entry := range e.dnsCache {
			e.deleteDNSCacheEntryLocked(k, entry)
			evicted = true
			break
		}
		if !evicted {
			break
		}
	}

	e.dnsCache[key] = dnsCacheEntry{
		Response:  response,
		ExpiresAt: expiresAt,
		SizeBytes: entrySize,
	}
	e.dnsCacheBytes += entrySize
}

func (e *Engine) dnsCacheByteLimit() int64 {
	if e != nil && e.dnsCacheMaxBytes > 0 {
		return e.dnsCacheMaxBytes
	}
	return defaultDNSCacheMaxBytes
}

func (e *Engine) deleteDNSCacheEntryLocked(key string, entry dnsCacheEntry) {
	delete(e.dnsCache, key)
	e.dnsCacheBytes -= entry.SizeBytes
	if e.dnsCacheBytes < 0 {
		e.dnsCacheBytes = 0
	}
}

func (e *Engine) closeIdleDNSPool() {
	e.drainDNSPool(false)
}

func (e *Engine) closeDNSPool() {
	e.drainDNSPool(true)
}

func (e *Engine) drainDNSPool(permanent bool) {
	e.dnsPoolMu.Lock()
	if permanent {
		e.dnsPoolClosed = true
	}
	if len(e.dnsPool) == 0 {
		e.dnsPoolMu.Unlock()
		return
	}
	var closers []io.Closer
	for _, conns := range e.dnsPool {
		for _, idle := range conns {
			closers = append(closers, idle.conn)
		}
	}
	e.dnsPool = make(map[string][]idleDNSConn)
	e.dnsPoolMu.Unlock()

	for _, c := range closers {
		_ = c.Close()
	}
}

/* Extracts the smallest cacheable TTL from a DNS response. */
func extractMinDNSTTL(payload []byte) int {
	if len(payload) < 12 {
		return 0
	}
	/* Accepts only complete responses. */
	if payload[2]&0x80 == 0 || payload[2]&0x02 != 0 {
		return 0
	}
	/* Accepts only NOERROR and NXDOMAIN for caching. */
	rcode := payload[3] & 0x0F
	if rcode != 0 && rcode != 3 {
		return 0
	}

	qdcount := int(binary.BigEndian.Uint16(payload[4:6]))
	ancount := int(binary.BigEndian.Uint16(payload[6:8]))
	nscount := int(binary.BigEndian.Uint16(payload[8:10]))
	arcount := int(binary.BigEndian.Uint16(payload[10:12]))

	offset := 12
	/* Advances past the question section. */
	for i := 0; i < qdcount; i++ {
		for offset < len(payload) {
			b := payload[offset]
			if b == 0 {
				offset++
				break
			} else if b&0xC0 == 0xC0 {
				offset += 2
				break
			}
			labelLen := int(b)
			if offset+labelLen+1 > len(payload) {
				return 0 /* Rejects malformed question labels. */
			}
			offset += labelLen + 1
		}
		if offset+4 > len(payload) {
			return 0 /* Rejects truncated question metadata. */
		}
		offset += 4 /* Advances past QTYPE and QCLASS. */
	}

	minTTL := -1

	/* Scans answer, authority, and additional records, but only answer/authority TTLs are cache-significant. */
	totalRecords := ancount + nscount + arcount
	for i := 0; i < totalRecords; i++ {
		if offset >= len(payload) {
			return 0 /* Rejects truncated responses that stop before all declared records. */
		}
		/* Advances past the record name. */
		if payload[offset]&0xC0 == 0xC0 {
			offset += 2
		} else {
			for offset < len(payload) {
				b := payload[offset]
				if b == 0 {
					offset++
					break
				} else if b&0xC0 == 0xC0 {
					offset += 2
					break
				}
				labelLen := int(b)
				if offset+labelLen+1 > len(payload) {
					return 0 /* Rejects malformed record names. */
				}
				offset += labelLen + 1
			}
		}

		if offset+10 > len(payload) {
			return 0 /* Rejects truncated resource-record metadata. */
		}
		rrType := binary.BigEndian.Uint16(payload[offset : offset+2])
		ttl := int(int32(binary.BigEndian.Uint32(payload[offset+4 : offset+8])))
		rdlength := int(binary.BigEndian.Uint16(payload[offset+8 : offset+10]))
		if offset+10+rdlength > len(payload) {
			return 0 /* Rejects truncated record payloads so malformed responses are never cached. */
		}
		offset += 10 + rdlength

		/* Keeps the smallest positive TTL. */
		if i < ancount+nscount && rrType != 41 && ttl > 0 {
			if minTTL == -1 || ttl < minTTL {
				minTTL = ttl
			}
		}
	}

	/* Uses a short cap for negative caching. */
	if rcode == 3 {
		if minTTL == -1 || minTTL > 60 {
			return 60 /* Caps NXDOMAIN caching at 60 seconds. */
		}
		return minTTL
	}

	if minTTL == -1 {
		return 0
	}
	return minTTL
}

func (e *Engine) takeIdleUDPSession(target string) UDPSession {
	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	e.udpPoolMu.Lock()
	if e.udpPool == nil {
		e.udpPoolMu.Unlock()
		return nil
	}
	sessions := e.udpPool[target]
	n := len(sessions)
	if n == 0 {
		e.udpPoolMu.Unlock()
		return nil
	}
	session := sessions[n-1]
	if n == 1 {
		delete(e.udpPool, target)
	} else {
		e.udpPool[target] = sessions[:n-1]
	}
	e.udpPoolMu.Unlock()
	if !e.activateUpstream(session) {
		return nil
	}
	return session
}

func udpSessionPoolKey(source, target string) string {
	if source == "" {
		return target
	}
	if target == "" {
		return source
	}
	return source + "|" + target
}

func shouldDropProxiedUDP(target *net.UDPAddr) bool {
	return target != nil && target.Port == 443
}

/* Returns a UDP session to the idle pool or discards it if the pool is full. */
func (e *Engine) releaseUDPSession(target string, session UDPSession, reusable bool) {
	if session == nil {
		return
	}
	if !reusable {
		e.discardUDPSession(session)
		return
	}
	if err := session.SetDeadline(time.Time{}); err != nil {
		e.discardUDPSession(session)
		return
	}

	e.upstreamLifecycleMu.RLock()
	defer e.upstreamLifecycleMu.RUnlock()
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	if stopping {
		e.discardUDPSession(session)
		return
	}

	e.udpPoolMu.Lock()
	if e.udpPoolClosed {
		e.udpPoolMu.Unlock()
		e.discardUDPSession(session)
		return
	}
	if e.udpPool == nil {
		e.udpPool = make(map[string][]UDPSession)
	}
	totalIdle := 0
	for _, sessions := range e.udpPool {
		totalIdle += len(sessions)
	}
	if totalIdle >= udpPoolMaxIdle {
		e.udpPoolMu.Unlock()
		e.discardUDPSession(session)
		return
	}
	if !e.unregisterTrackedUpstream(session) {
		e.udpPoolMu.Unlock()
		_ = session.Close()
		return
	}
	e.udpPool[target] = append(e.udpPool[target], session)
	e.udpPoolMu.Unlock()
}

func (e *Engine) discardUDPSession(session UDPSession) {
	if session == nil {
		return
	}
	e.unregisterActiveUpstream(session)
	_ = session.Close()
}

func (e *Engine) closeIdleUDPPool() {
	e.drainUDPPool(false)
}

func (e *Engine) closeUDPPool() {
	e.drainUDPPool(true)
}

func (e *Engine) drainUDPPool(permanent bool) {
	e.udpPoolMu.Lock()
	if permanent {
		e.udpPoolClosed = true
	}
	if len(e.udpPool) == 0 {
		e.udpPoolMu.Unlock()
		return
	}
	var sessions []UDPSession
	for _, idle := range e.udpPool {
		sessions = append(sessions, idle...)
	}
	e.udpPool = nil
	e.udpPoolMu.Unlock()

	for _, session := range sessions {
		e.discardUDPSession(session)
	}
}

func (e *Engine) shouldReuseUDPSession(ctx context.Context, errs ...error) bool {
	if ctx.Err() != nil {
		return false
	}
	for _, err := range errs {
		if err == nil || isClosedConnError(err) || isTimeoutError(err) {
			continue
		}
		return false
	}
	return true
}

func flowResultString(err error) string {
	if err == nil || errors.Is(err, io.EOF) || isClosedConnError(err) {
		return "ok"
	}
	if isBenignLocalEndpointClosedError(err) {
		return "local-closed"
	}
	if isTimeoutError(err) {
		return "timeout"
	}
	return err.Error()
}

func isBenignLocalEndpointClosedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "endpoint is closed for send") ||
		strings.Contains(msg, "endpoint is closed for receive")
}

func dnsSummaryErrors(upErr, downErr error) (error, error) {
	if errors.Is(upErr, io.EOF) {
		upErr = io.ErrUnexpectedEOF
	}
	if errors.Is(downErr, io.EOF) {
		downErr = io.ErrUnexpectedEOF
	}
	return upErr, downErr
}

func combineResults(upErr, downErr error) string {
	up := flowResultString(upErr)
	down := flowResultString(downErr)
	if up == "ok" && down == "ok" {
		return "ok"
	}
	return "up:" + up + "/down:" + down
}

func combineUDPResults(upRes, downRes udpPumpResult) string {
	up := udpFlowResultString(upRes.Err)
	if upRes.LocalWriteErr && isBenignLocalPacketWriteError(upRes.Err) {
		up = "local-closed"
	}
	down := udpFlowResultString(downRes.Err)
	if downRes.LocalWriteErr && isBenignLocalPacketWriteError(downRes.Err) {
		down = "local-closed"
	}
	if udpNoUpstreamResponse(upRes, downRes) {
		down = "no-response"
	}
	if up == "ok" && down == "ok" {
		return "ok"
	}
	return "up:" + up + "/down:" + down
}

func udpFlowResultString(err error) string {
	if isTimeoutError(err) {
		return "idle"
	}
	return flowResultString(err)
}

func udpNoUpstreamResponse(upRes, downRes udpPumpResult) bool {
	return upRes.Bytes > 0 && upRes.Packets > 0 && downRes.Bytes == 0 && downRes.Packets == 0
}

type closeWriter interface{ CloseWrite() error }
type closeReader interface{ CloseRead() error }

func halfCloseWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func halfCloseRead(conn net.Conn) {
	if cr, ok := conn.(closeReader); ok {
		_ = cr.CloseRead()
	}
}

func (e *Engine) clearLogThrottleStates() {
	e.logThrottleMu.Lock()
	e.logThrottleStates = make(map[string]logThrottleState)
	e.logThrottleMu.Unlock()
}

func getPooledBuf(pool *relayBufferPool, size int) *[]byte {
	classIndex, classSize, pooled := relayBufferClass(size)
	if pool == nil || !pooled {
		buf := make([]byte, size)
		return &buf
	}
	if v := pool.classes[classIndex].Get(); v != nil {
		if buf, ok := v.(*[]byte); ok && cap(*buf) == classSize {
			*buf = (*buf)[:size]
			return buf
		}
	}
	buf := make([]byte, size, classSize)
	return &buf
}

func putPooledBuf(pool *relayBufferPool, buf *[]byte) {
	if pool == nil || buf == nil {
		return
	}
	classIndex, classSize, pooled := relayBufferClass(cap(*buf))
	if !pooled || cap(*buf) != classSize {
		return
	}
	*buf = (*buf)[:classSize]
	pool.classes[classIndex].Put(buf)
}

func relayBufferClass(size int) (index, classSize int, pooled bool) {
	if size <= 0 || size > maxPooledRelayBufferBytes {
		return 0, 0, false
	}
	classSize = minPooledRelayBufferBytes
	for index = 0; index < relayBufferPoolClasses-1 && classSize < size; index++ {
		classSize <<= 1
	}
	return index, classSize, true
}

/* ----------------------------------------------------------------------------- */
/* gVisor netstack wiring */
/* ----------------------------------------------------------------------------- */

/* Netstack integration. */
type NetstackOptions struct {
	TCPHandler func(id gstack.TransportEndpointID, conn net.Conn)
	UDPHandler func(id gstack.TransportEndpointID, conn net.PacketConn)
	EnableUDP  bool
}

type NetstackRuntime struct {
	Stack *gstack.Stack
	NICID tcpip.NICID
}

/* Creates a single NIC stack and attaches TCP/UDP forwarders. */
func NewNetstack(linkEP gstack.LinkEndpoint, opts NetstackOptions) (*NetstackRuntime, error) {
	s := gstack.New(gstack.Options{
		NetworkProtocols: []gstack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []gstack.TransportProtocolFactory{
			tcp.NewProtocol,
			udp.NewProtocol,
			icmp.NewProtocol4,
			icmp.NewProtocol6,
		},
	})

	nicID := tcpip.NICID(1)
	if err := s.CreateNICWithOptions(nicID, linkEP, gstack.NICOptions{}); err != nil {
		return nil, fmt.Errorf("create nic: %s", err)
	}
	if err := s.SetPromiscuousMode(nicID, true); err != nil {
		return nil, fmt.Errorf("set promiscuous mode: %s", err)
	}
	if err := s.SetSpoofing(nicID, true); err != nil {
		return nil, fmt.Errorf("set spoofing: %s", err)
	}

	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})

	installTCPForwarder(s, opts.TCPHandler)
	if opts.EnableUDP {
		installUDPForwarder(s, opts.UDPHandler)
	}

	return &NetstackRuntime{Stack: s, NICID: nicID}, nil
}

/* Accepts TCP flows from netstack and delegates handling. */
func installTCPForwarder(s *gstack.Stack, handle func(id gstack.TransportEndpointID, conn net.Conn)) {
	fwd := tcp.NewForwarder(s, 0, 2048, func(req *tcp.ForwarderRequest) {
		if handle == nil {
			req.Complete(true)
			return
		}

		var wq waiter.Queue
		ep, err := req.CreateEndpoint(&wq)
		if err != nil {
			if isBenignTCPForwarderEndpointError(err) {
				logf(LogLevelDebug, "[DEBUG] [TCP] tcp handshake ended before endpoint creation: %v", err)
			} else {
				logf(LogLevelError, "[ERROR] [TCP] tcp create endpoint failed: %v", err)
			}
			req.Complete(true)
			return
		}
		id := req.ID()
		req.Complete(false)

		conn := gonet.NewTCPConn(&wq, ep)
		go handle(id, conn)
	})

	s.SetTransportProtocolHandler(tcp.ProtocolNumber, fwd.HandlePacket)
}

func isBenignTCPForwarderEndpointError(err tcpip.Error) bool {
	_, ok := err.(*tcpip.ErrConnectionRefused)
	return ok
}

/* Accepts UDP flows from netstack and delegates handling. */
func installUDPForwarder(s *gstack.Stack, handle func(id gstack.TransportEndpointID, conn net.PacketConn)) {
	fwd := udp.NewForwarder(s, func(req *udp.ForwarderRequest) bool {
		if handle == nil {
			return false
		}

		var wq waiter.Queue
		ep, err := req.CreateEndpoint(&wq)
		if err != nil {
			logf(LogLevelError, "[ERROR] [UDP] udp create endpoint failed: %v", err)
			return false
		}
		id := req.ID()
		conn := gonet.NewUDPConn(&wq, ep)
		go handle(id, conn)

		return true
	})

	s.SetTransportProtocolHandler(udp.ProtocolNumber, fwd.HandlePacket)
}

func tcpDestination(id gstack.TransportEndpointID) string {
	host := ipString(id.LocalAddress.AsSlice())
	return net.JoinHostPort(host, strconv.Itoa(int(id.LocalPort)))
}

func tcpSource(id gstack.TransportEndpointID) string {
	host := ipString(id.RemoteAddress.AsSlice())
	return net.JoinHostPort(host, strconv.Itoa(int(id.RemotePort)))
}

func udpDestination(id gstack.TransportEndpointID) *net.UDPAddr {
	ip := append(net.IP(nil), id.LocalAddress.AsSlice()...)
	return &net.UDPAddr{IP: ip, Port: int(id.LocalPort)}
}

func udpDestinationString(id gstack.TransportEndpointID) string {
	host := ipString(id.LocalAddress.AsSlice())
	return net.JoinHostPort(host, strconv.Itoa(int(id.LocalPort)))
}

func udpSourceString(id gstack.TransportEndpointID) string {
	host := ipString(id.RemoteAddress.AsSlice())
	return net.JoinHostPort(host, strconv.Itoa(int(id.RemotePort)))
}

func ipString(raw []byte) string {
	if len(raw) == 0 {
		return "0.0.0.0"
	}
	ip := net.IP(make([]byte, len(raw)))
	copy(ip, raw)
	return ip.String()
}

func formatBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	if n < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(n)/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB", float64(n)/(1024*1024*1024))
}

func formatDuration(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}
