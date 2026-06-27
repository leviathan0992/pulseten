package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	autoRouteEnsureFastInterval     = 1 * time.Second
	autoRouteEnsureSlowInterval     = 3 * time.Second
	autoRouteEnsureSlowAfterSuccess = 5
)

/* ----------------------------------------------------------------------------- */
/* Application lifecycle */
/* ----------------------------------------------------------------------------- */

/* Process lifecycle manager. */
type App struct {
	version string
}

/* Creates an application instance with build/version metadata. */
func NewApp(version string) *App {
	return &App{version: version}
}

/* Loads config, starts the engine, and blocks until a shutdown signal. */
func (a *App) Run(cfgPath, tunOverride string, debug bool) error {
	level := LogLevelInfo
	if debug {
		level = LogLevelDebug
	}
	SetLogLevel(level)

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return err
	}
	if err := cfg.ApplyOverrides("", tunOverride); err != nil {
		return err
	}
	if cfg.AutoRoute {
		pinnedHost, err := cfg.PinUpstreamIPForAutoRoute()
		if err != nil {
			return err
		}
		if pinnedHost != "" {
			logf(
				LogLevelSystem,
				"[SYS] [NET] auto-route pinned upstream host=%s upstreams=%s",
				pinnedHost,
				cfg.UpstreamAddrsString(),
			)
		}
	}

	logf(
		LogLevelSystem,
		"[SYS] shamantun version=%s mode=%s tun=%s upstreams=%s",
		a.version,
		cfg.Mode,
		cfg.Tun.Name,
		cfg.UpstreamAddrsString(),
	)
	if debug {
		logf(LogLevelDebug, "[DEBUG] verbose per-flow logging enabled")
	}
	if cfg.Upstream.InsecureSkipVerify {
		logf(LogLevelWarn, "[WARN] upstream tls verification is disabled (insecure_skip_verify=true)")
	}
	if cfg.AutoRoute && !cfg.Runtime.EnableUDP {
		logf(LogLevelWarn, "[WARN] mode=%s with auto-route and enable_udp=false may break DNS on systems that use UDP resolvers", cfg.Mode)
	}
	if cfg.Mode == ModeHTTPS {
		if cfg.Runtime.EnableUDP {
			logf(LogLevelWarn, "[WARN] mode=%s relays DNS udp/53 over proxied TCP; other UDP traffic is not forwarded", cfg.Mode)
		} else {
			logf(LogLevelWarn, "[WARN] mode=%s only proxies TCP because enable_udp=false; UDP traffic is not captured", cfg.Mode)
		}
	}

	eng, err := NewEngine(cfg)
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}
	logf(LogLevelSystem, "[SYS] config: connect_timeout=%s idle_timeout=%s udp_idle_timeout=%s tcp_buffer=%s udp_buffer=%s active_relay_buffer_estimate=%s dns_cache=%d dns_cache_payload_budget=%s tcp_dial_concurrency=%d udp_dial_concurrency=%d tcp_handler_concurrency=%d udp_handler_concurrency=%d enable_udp=%v routing_enabled=%v routing_default=%s routing_direct_cidrs=%d routing_direct_files=%d routing_proxy_cidrs=%d routing_proxy_files=%d",
		cfg.ConnectTimeout(),
		cfg.IdleTimeout(),
		cfg.UDPIdleTimeout(),
		formatBytes(int64(cfg.Runtime.TCPBuffer)),
		formatBytes(int64(cfg.Runtime.UDPBuffer)),
		formatBytes(cfg.EstimatedRelayBufferBytes()),
		cfg.Runtime.DNSCacheSize,
		formatBytes(defaultDNSCacheMaxBytes),
		cfg.Runtime.TCPDialConcurrency,
		cfg.Runtime.UDPDialConcurrency,
		cfg.Runtime.TCPHandlerConcurrency,
		cfg.Runtime.UDPHandlerConcurrency,
		cfg.Runtime.EnableUDP,
		cfg.Routing.Enabled,
		cfg.Routing.DefaultOutbound,
		len(cfg.Routing.DirectCIDRs),
		len(cfg.Routing.DirectCIDRFiles),
		len(cfg.Routing.ProxyCIDRs),
		len(cfg.Routing.ProxyCIDRFiles),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var routeMgr *AutoRouteManager
	var ensureDone chan struct{}
	var fatalRouteErr chan error
	if err := eng.StartWithPreNetstackHook(ctx, func(tunName string) error {
		if !cfg.AutoRoute {
			return nil
		}
		routeMgr, err = NewAutoRouteManager(cfg, tunName)
		if err != nil {
			return err
		}
		if err := routeMgr.Setup(); err != nil {
			return err
		}
		eng.SetDirectRouteHook(routeMgr.EnsureDirectRoute)
		return nil
	}); err != nil {
		if routeMgr != nil {
			eng.SetDirectRouteHook(nil)
			_ = routeMgr.Teardown()
		}
		return err
	}

	if cfg.AutoRoute {
		fatalRouteErr = make(chan error, 1)
		routeChangeCh := make(chan struct{}, 1)
		routeMgr.SetDefaultRouteChangeHook(func() {
			select {
			case routeChangeCh <- struct{}{}:
			default:
			}
		})
		upstreamBypassIPs := routeMgr.effectiveUpstreamIPs()
		if len(upstreamBypassIPs) == 0 {
			logf(LogLevelSystem, "[SYS] [NET] auto-route enabled for tun=%s upstream-bypass=skipped (local upstream route is handled by host)", eng.tunDev.Name)
		} else {
			logf(LogLevelSystem, "[SYS] [NET] auto-route enabled for tun=%s upstream-ips=%s", eng.tunDev.Name, strings.Join(upstreamBypassIPs, ","))
		}

		ensureDone = make(chan struct{})
		go func() {
			defer close(ensureDone)
			stablePasses := 0
			ensureRoutes := func() bool {
				if err := routeMgr.Ensure(); err != nil {
					stablePasses = 0
					if handleFatalAutoRouteError(err, eng.Quiesce, cancel) {
						select {
						case fatalRouteErr <- err:
						default:
						}
						logf(LogLevelError, "[ERROR] [NET] auto-route safety conflict; shutting down to prevent direct traffic: tun=%s upstream=%s err=%v", eng.tunDev.Name, routeMgr.upstreamIP, err)
						return false
					}
					logf(LogLevelWarn, "[WARN] [NET] auto-route ensure warning: tun=%s upstream=%s err=%v", eng.tunDev.Name, routeMgr.upstreamIP, err)
					return true
				}
				if stablePasses < autoRouteEnsureSlowAfterSuccess {
					stablePasses++
				}
				return true
			}
			timer := time.NewTimer(nextAutoRouteEnsureInterval(stablePasses))
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-routeChangeCh:
					eng.HandleNetworkChange()
					stablePasses = 0
					if !ensureRoutes() {
						return
					}
					resetTimer(timer, nextAutoRouteEnsureInterval(stablePasses))
				case <-timer.C:
					if !ensureRoutes() {
						return
					}
					resetTimer(timer, nextAutoRouteEnsureInterval(stablePasses))
				}
			}
		}()
	} else {
		logf(LogLevelSystem, "[SYS] [NET] auto-route disabled")
	}

	<-ctx.Done()
	logf(LogLevelSystem, "[SYS] shutdown signal received")

	/* Shortens shutdown waits after a second shutdown signal without skipping route teardown. */
	forceQuit := make(chan os.Signal, 1)
	signal.Notify(forceQuit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(forceQuit)
	forcedShutdown := make(chan struct{})
	var forceOnce sync.Once
	go func() {
		<-forceQuit
		forceOnce.Do(func() {
			logf(LogLevelSystem, "[SYS] forced shutdown")
			close(forcedShutdown)
		})
	}()

	if routeMgr != nil {
		routeMgr.SetDefaultRouteChangeHook(nil)
		eng.SetDirectRouteHook(nil)
	}

	if ensureDone != nil {
		select {
		case <-ensureDone:
		case <-forcedShutdown:
			timer := time.NewTimer(250 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ensureDone:
			case <-timer.C:
				_, _ = fmt.Fprintln(os.Stderr, "shutdown warning: continuing to route teardown before ensure loop fully drained")
			}
		}
	}
	if routeMgr != nil {
		if err := routeMgr.Teardown(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "route teardown warning: %v\n", err)
		}
	}

	shutdownTimeout := cfg.ConnectTimeout()
	if shutdownTimeout <= 0 || shutdownTimeout > 5*time.Second {
		shutdownTimeout = 5 * time.Second
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownTimeout)
	defer stop()
	go func() {
		select {
		case <-forcedShutdown:
			stop()
		case <-shutdownCtx.Done():
		}
	}()
	if err := eng.Stop(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "shutdown warning: %v\n", err)
	}

	if err := autoRouteRunError(fatalRouteErr); err != nil {
		return err
	}
	return nil
}

func nextAutoRouteEnsureInterval(stablePasses int) time.Duration {
	if stablePasses >= autoRouteEnsureSlowAfterSuccess {
		return autoRouteEnsureSlowInterval
	}
	return autoRouteEnsureFastInterval
}

func handleFatalAutoRouteError(err error, quiesce func(), cancel context.CancelFunc) bool {
	if !isAutoRouteProxyBypassConflict(err) {
		return false
	}
	if quiesce != nil {
		quiesce()
	}
	if cancel != nil {
		cancel()
	}
	return true
}

func autoRouteRunError(fatalErr <-chan error) error {
	if fatalErr == nil {
		return nil
	}
	select {
	case err := <-fatalErr:
		return fmt.Errorf("auto-route safety shutdown: %w", err)
	default:
		return nil
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}
