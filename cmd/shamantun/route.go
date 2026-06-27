package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var autoRoutePrivateBypassCIDRs = []string{
	"10.0.0.0/8",
	"100.64.0.0/10",
	"100.100.0.0/16",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
	"fc00::/7",
	"fe80::/10",
}

const (
	routeCommandTimeout = 3 * time.Second
	maxDirectHostRoutes = 2048
)

var execCommandContext = exec.CommandContext

/* Applies and restores system routes without external scripts. */
type AutoRouteManager struct {
	mu         sync.Mutex
	onChangeMu sync.RWMutex
	commandMu  sync.Mutex
	routeWG    sync.WaitGroup
	cfg        *Config
	tunName    string

	upstreamIP  string
	upstreamIPs []string
	proxyCIDRs  []*net.IPNet
	onChange    func()

	lanCIDRs                 []string
	installedLANCIDRs        []string
	installedLANDevs         map[string]string
	installedLANIFs          map[string]string
	installedDirectHosts     map[string]directCIDRRouteState
	installedDirectHostOrder []string
	pendingDirectHosts       map[string]*directHostRoutePending
	tearingDown              bool
	setupMutated             bool
	teardownRouteErr         error

	linuxDefaultRoute   string
	linuxDefaultGW      string
	linuxDefaultDev     string
	linuxDefaultRouteV6 string
	linuxDefaultGWV6    string
	linuxDefaultDevV6   string

	darwinDefaultGW   string
	darwinDefaultIF   string
	darwinDefaultGWV6 string
	darwinDefaultIFV6 string
}

type directCIDRRouteState struct {
	gateway string
	dev     string
}

type directHostRoutePending struct {
	done chan struct{}
	err  error
}

type autoRouteProxyBypassConflictError struct {
	proxyCIDR  string
	bypassCIDR string
}

func (e *autoRouteProxyBypassConflictError) Error() string {
	return fmt.Sprintf(
		"auto-route conflicts with routing.proxy_cidrs: proxy CIDR %s overlaps direct bypass %s",
		e.proxyCIDR,
		e.bypassCIDR,
	)
}

func isAutoRouteProxyBypassConflict(err error) bool {
	var conflict *autoRouteProxyBypassConflictError
	return errors.As(err, &conflict)
}

/* Installs a callback fired when the observed default route changes. */
func (m *AutoRouteManager) SetDefaultRouteChangeHook(fn func()) {
	m.onChangeMu.Lock()
	m.onChange = fn
	m.onChangeMu.Unlock()
}

/* Prepares per-platform route automation state. */
func NewAutoRouteManager(cfg *Config, tunName string) (*AutoRouteManager, error) {
	if cfg == nil {
		return nil, errors.New("nil config")
	}
	if strings.TrimSpace(tunName) == "" {
		return nil, errors.New("auto-route requires a non-empty tun name")
	}
	var proxyCIDRs []*net.IPNet
	if cfg.Routing.Enabled {
		var err error
		proxyCIDRs, err = loadCIDRRules(cfg.Routing.ProxyCIDRs, cfg.Routing.ProxyCIDRFiles)
		if err != nil {
			return nil, fmt.Errorf("load routing.proxy_cidrs for auto-route validation: %w", err)
		}
		if err := validateProxyCIDRsAgainstAutoBypass(proxyCIDRs, autoRoutePrivateBypassCIDRs); err != nil {
			return nil, err
		}
	}

	var upstreamIPs []string
	for _, upstream := range cfg.Upstreams {
		upstreamIP, err := resolveUpstreamIP(upstream.Addr)
		if err != nil {
			return nil, err
		}
		if ip := net.ParseIP(upstreamIP); ip != nil {
			if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
				continue
			}
		}
		if !slices.Contains(upstreamIPs, upstreamIP) {
			upstreamIPs = append(upstreamIPs, upstreamIP)
		}
	}
	upstreamIP := ""
	if len(upstreamIPs) > 0 {
		upstreamIP = upstreamIPs[0]
	}
	return &AutoRouteManager{
		cfg:         cfg,
		tunName:     tunName,
		upstreamIP:  upstreamIP,
		upstreamIPs: upstreamIPs,
		proxyCIDRs:  proxyCIDRs,
	}, nil
}

/* Applies routes for the current platform. */
func (m *AutoRouteManager) Setup() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if os.Geteuid() != 0 {
		return errors.New("auto-route requires root privileges (run with sudo)")
	}
	m.tearingDown = false
	m.setupMutated = false
	m.teardownRouteErr = nil

	switch runtime.GOOS {
	case "linux":
		return m.setupLinux()
	case "darwin":
		return m.setupDarwin()
	default:
		return fmt.Errorf("auto-route unsupported on %s", runtime.GOOS)
	}
}

/* Reverts routes for the current platform. */
func (m *AutoRouteManager) Teardown() error {
	m.mu.Lock()
	if os.Geteuid() != 0 {
		m.mu.Unlock()
		return nil
	}
	m.tearingDown = true
	if !m.setupMutated {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	m.routeWG.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	var platformErr error
	switch runtime.GOOS {
	case "linux":
		platformErr = m.teardownLinux()
	case "darwin":
		platformErr = m.teardownDarwin()
	}
	m.setupMutated = false
	return errors.Join(m.teardownRouteErr, platformErr)
}

/* Periodically repairs platform routes if other software rewrites them. */
func (m *AutoRouteManager) Ensure() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if os.Geteuid() != 0 {
		return nil
	}

	switch runtime.GOOS {
	case "linux":
		return m.ensureLinux()
	case "darwin":
		return m.ensureDarwin()
	default:
		return nil
	}
}

func (m *AutoRouteManager) EnsureDirectRoute(route DirectRoute) error {
	if m == nil || route.Decision.Outbound != OutboundDirect {
		return nil
	}
	ip := normalizeIP(route.IP)
	if ip == nil {
		return nil
	}
	if os.Geteuid() != 0 {
		return nil
	}

	m.mu.Lock()
	if m.tearingDown || m.hostCoveredByBypassLocked(ip) {
		m.mu.Unlock()
		return nil
	}
	err := m.ensureDirectHostRouteLocked(ip)
	m.mu.Unlock()
	return err
}

func (m *AutoRouteManager) setupLinux() error {
	cidr, err := linuxTunCIDR(m.cfg.Tun.IPv4)
	if err != nil {
		return err
	}
	cidrV6, err := linuxTunIPv6CIDR(m.cfg.Tun.IPv6)
	if err != nil {
		return err
	}

	defaultRouteLine, defaultGW, defaultDev, err := snapshotLinuxDefaultRoute()
	if err != nil {
		return err
	}
	if defaultDev == "" {
		return errors.New("snapshot linux default route: empty default interface")
	}
	m.linuxDefaultRoute = defaultRouteLine
	m.linuxDefaultGW = defaultGW
	m.linuxDefaultDev = defaultDev
	defaultRouteLineV6, defaultGWV6, defaultDevV6, err := snapshotLinuxDefaultRouteV6()
	if err != nil {
		return err
	}
	m.linuxDefaultRouteV6 = defaultRouteLineV6
	m.linuxDefaultGWV6 = defaultGWV6
	m.linuxDefaultDevV6 = defaultDevV6
	if err := m.validateLinuxProxyBypass(defaultDev, defaultDevV6); err != nil {
		return err
	}

	runCommandIgnore("ip", "addr", "add", cidr, "dev", m.tunName)
	if m.linuxDefaultRouteV6 != "" {
		runCommandIgnore("ip", "-6", "addr", "add", cidrV6, "dev", m.tunName)
	}

	if err := runCommand("ip", "link", "set", "dev", m.tunName, "up"); err != nil {
		return err
	}

	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		upstreamGW, upstreamDev, err := m.linuxUpstreamRoute(upstreamIP)
		if err != nil {
			return err
		}
		m.setupMutated = true
		if err := replaceLinuxHostRoute(upstreamIP, upstreamGW, upstreamDev); err != nil {
			return err
		}
	}
	m.setupMutated = true
	if err := runCommand("ip", "route", "replace", "default", "dev", m.tunName); err != nil {
		return err
	}
	if m.linuxDefaultRouteV6 != "" {
		if err := runCommand("ip", "-6", "route", "replace", "default", "dev", m.tunName); err != nil {
			return err
		}
	}

	if err := m.refreshLinuxLANBypass(m.linuxDefaultDev, m.linuxDefaultDevV6); err != nil {
		return err
	}
	return nil
}

func (m *AutoRouteManager) teardownLinux() error {
	defer m.resetInstalledLANState()

	runCommandIgnore("ip", "route", "del", "default", "dev", m.tunName)
	if m.linuxDefaultRouteV6 != "" {
		runCommandIgnore("ip", "-6", "route", "del", "default", "dev", m.tunName)
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		runCommandIgnoreArgs(linuxDeleteHostRouteArgs(upstreamIP))
	}
	for host := range m.installedDirectHosts {
		runCommandIgnoreArgs(linuxDeleteHostRouteArgs(host))
	}
	for _, cidr := range m.installedLANCIDRs {
		m.removeLinuxManagedRoute(cidr)
	}

	var retErr error
	if m.linuxDefaultRoute != "" {
		args := append([]string{"route", "replace"}, strings.Fields(m.linuxDefaultRoute)...)
		if err := runCommand("ip", args...); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore linux default route: %w", err))
		}
	}
	if m.linuxDefaultRouteV6 != "" {
		args := append([]string{"-6", "route", "replace"}, strings.Fields(m.linuxDefaultRouteV6)...)
		if err := runCommand("ip", args...); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore linux default route v6: %w", err))
		}
	}

	runCommandIgnore("ip", "link", "set", "dev", m.tunName, "down")

	return retErr
}

func (m *AutoRouteManager) ensureLinux() error {
	routeLine := m.linuxDefaultRoute
	routeGW := m.linuxDefaultGW
	routeDev := m.linuxDefaultDev
	routeLineV6 := m.linuxDefaultRouteV6
	routeGWV6 := m.linuxDefaultGWV6
	routeDevV6 := m.linuxDefaultDevV6
	changed := false
	changedV6 := false

	defaultRouteOutput, err := runCommandOutput("ip", "route", "show", "default")
	if err == nil {
		observedRouteLine, observedRouteGW, observedRouteDev := selectLinuxBypassRoute(defaultRouteOutput, m.tunName)
		if observedRouteLine != "" && observedRouteDev != "" {
			routeLine = observedRouteLine
			routeGW = observedRouteGW
			routeDev = observedRouteDev
			changed = routeLine != m.linuxDefaultRoute || routeGW != m.linuxDefaultGW || routeDev != m.linuxDefaultDev
		}
	}
	if m.linuxDefaultRouteV6 != "" {
		defaultRouteOutputV6, err := runCommandOutput("ip", "-6", "route", "show", "default")
		if err == nil {
			observedRouteLineV6, observedRouteGWV6, observedRouteDevV6 := selectLinuxBypassRoute(defaultRouteOutputV6, m.tunName)
			if observedRouteLineV6 != "" && observedRouteDevV6 != "" {
				routeLineV6 = observedRouteLineV6
				routeGWV6 = observedRouteGWV6
				routeDevV6 = observedRouteDevV6
				changedV6 = routeLineV6 != m.linuxDefaultRouteV6 || routeGWV6 != m.linuxDefaultGWV6 || routeDevV6 != m.linuxDefaultDevV6
			}
		}
	}
	if routeLine != "" && routeDev != "" {
		m.commitLinuxDefaultRoute(routeLine, routeGW, routeDev, changed)
	}
	if routeLineV6 != "" && routeDevV6 != "" {
		m.commitLinuxDefaultRouteV6(routeLineV6, routeGWV6, routeDevV6, changedV6)
	}
	if err := m.validateLinuxProxyBypass(routeDev, routeDevV6); err != nil {
		return err
	}

	if err := runCommand("ip", "route", "replace", "default", "dev", m.tunName); err != nil {
		return fmt.Errorf("ensure linux split default route: %w", err)
	}
	if m.linuxDefaultRouteV6 != "" {
		if err := runCommand("ip", "-6", "route", "replace", "default", "dev", m.tunName); err != nil {
			return fmt.Errorf("ensure linux split default route v6: %w", err)
		}
	}
	if routeDev != "" {
		for _, upstreamIP := range m.effectiveUpstreamIPs() {
			upstreamGW, upstreamDev := routeGW, routeDev
			if upstreamUsesIPv6(upstreamIP) {
				upstreamGW, upstreamDev = routeGWV6, routeDevV6
			}
			if strings.TrimSpace(upstreamDev) == "" {
				return fmt.Errorf("ensure linux upstream route %s: empty default device", upstreamIP)
			}
			if err := replaceLinuxHostRoute(upstreamIP, upstreamGW, upstreamDev); err != nil {
				return fmt.Errorf("ensure linux upstream route %s: %w", upstreamIP, err)
			}
		}
		if err := m.refreshLinuxLANBypass(routeDev, routeDevV6); err != nil {
			return err
		}
	}
	if err := m.refreshDirectHostRoutesLocked(); err != nil {
		return err
	}
	return nil
}

func (m *AutoRouteManager) setupDarwin() error {
	tunIP, tunMask, err := darwinTunIPMask(m.cfg.Tun.IPv4)
	if err != nil {
		return err
	}
	tunIPv6, tunPrefixV6, err := darwinTunIPv6(m.cfg.Tun.IPv6)
	if err != nil {
		return err
	}

	defaultGW, defaultIF, err := snapshotDarwinDefaultRoute()
	if err != nil {
		return err
	}
	if defaultIF == "" {
		return errors.New("snapshot darwin default route: empty interface")
	}
	m.darwinDefaultGW = defaultGW
	m.darwinDefaultIF = defaultIF
	defaultGWV6, defaultIFV6, err := snapshotDarwinDefaultRouteV6()
	if err != nil {
		return err
	}
	m.darwinDefaultGWV6 = defaultGWV6
	m.darwinDefaultIFV6 = defaultIFV6
	if err := m.validateDarwinProxyBypass(defaultIF, defaultIFV6); err != nil {
		return err
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		if _, _, err := m.darwinUpstreamRoute(upstreamIP); err != nil {
			return fmt.Errorf("snapshot darwin default route for upstream %s: %w", upstreamIP, err)
		}
	}
	if err := runCommand("ifconfig", m.tunName, "inet", tunIP, tunIP, "netmask", tunMask, "up"); err != nil {
		if errFallback := runCommand("ifconfig", m.tunName, "inet", tunIP, tunIP, "up"); errFallback != nil {
			return errors.Join(err, errFallback)
		}
	}
	if m.darwinDefaultIFV6 != "" {
		if err := runCommand("ifconfig", m.tunName, "inet6", tunIPv6, "prefixlen", strconv.Itoa(tunPrefixV6), "alias"); err != nil {
			return err
		}
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		upstreamGW, _, err := m.darwinUpstreamRoute(upstreamIP)
		if err != nil {
			return err
		}
		m.setupMutated = true
		runCommandIgnore("route", darwinDeleteHostRouteArgs(upstreamIP)...)
		if err := addOrChangeDarwinHostRoute(upstreamIP, upstreamGW); err != nil {
			return err
		}
	}
	m.setupMutated = true
	for _, splitNet := range []string{"0.0.0.0/1", "128.0.0.0/1"} {
		if err := addOrChangeDarwinSplitRoute(splitNet, m.tunName); err != nil {
			return err
		}
	}
	if m.darwinDefaultIFV6 != "" {
		for _, splitNetV6 := range []string{"::/1", "8000::/1"} {
			if err := addOrChangeDarwinSplitRouteV6(splitNetV6, m.tunName); err != nil {
				return err
			}
		}
	}
	if err := m.refreshDarwinLANBypass(m.darwinDefaultIF, m.darwinDefaultIFV6); err != nil {
		return err
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		routeGetOut, err := runCommandOutput("route", darwinRouteGetArgs(upstreamIP)...)
		if err != nil {
			return fmt.Errorf("validate darwin upstream route %s: %w", upstreamIP, err)
		}
		routeGW, routeIF := parseDarwinDefaultRoute(routeGetOut)
		logf(LogLevelDebug, "[DEBUG] [NET] darwin upstream route host=%s | gateway=%s interface=%s", upstreamIP, routeGW, routeIF)
		if routeIF == m.tunName {
			return fmt.Errorf("darwin upstream bypass route loop detected: host=%s is routed via %s", upstreamIP, m.tunName)
		}
	}

	return nil
}

func (m *AutoRouteManager) teardownDarwin() error {
	defer m.resetInstalledLANState()

	runCommandIgnore("route", "-n", "delete", "-net", "0.0.0.0/1")
	runCommandIgnore("route", "-n", "delete", "-net", "128.0.0.0/1")
	if m.darwinDefaultIFV6 != "" {
		runCommandIgnore("route", "-n", "delete", "-inet6", "-net", "::/1")
		runCommandIgnore("route", "-n", "delete", "-inet6", "-net", "8000::/1")
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		runCommandIgnore("route", darwinDeleteHostRouteArgs(upstreamIP)...)
	}
	for host := range m.installedDirectHosts {
		runCommandIgnore("route", darwinDeleteHostRouteArgs(host)...)
	}
	for _, cidr := range m.installedLANCIDRs {
		m.removeDarwinManagedRoute(cidr)
	}
	return nil
}

func (m *AutoRouteManager) resetInstalledLANState() {
	m.lanCIDRs = nil
	m.installedLANCIDRs = nil
	m.installedLANDevs = nil
	m.installedLANIFs = nil
	m.installedDirectHosts = nil
	m.installedDirectHostOrder = nil
	m.pendingDirectHosts = nil
}

func (m *AutoRouteManager) ensureDarwin() error {
	defaultOut, err := runCommandOutput("route", "-n", "get", "default")
	if err != nil {
		return fmt.Errorf("ensure darwin default route: %w", err)
	}
	defaultGW, defaultIF := parseDarwinDefaultRoute(defaultOut)
	if defaultGW == "" {
		defaultGW = m.darwinDefaultGW
	}
	if defaultIF == "" {
		defaultIF = m.darwinDefaultIF
	}
	if defaultIF == "" {
		return fmt.Errorf("ensure darwin default route: empty interface")
	}
	changed := defaultGW != m.darwinDefaultGW || defaultIF != m.darwinDefaultIF
	changedV6 := false
	defaultGWV6 := m.darwinDefaultGWV6
	defaultIFV6 := m.darwinDefaultIFV6
	if m.darwinDefaultIFV6 != "" {
		defaultOutV6, err := runCommandOutput("route", "-n", "get", "-inet6", "default")
		if err == nil {
			observedGWV6, observedIFV6 := parseDarwinDefaultRoute(defaultOutV6)
			if observedIFV6 != "" {
				defaultGWV6 = observedGWV6
				defaultIFV6 = observedIFV6
				changedV6 = defaultGWV6 != m.darwinDefaultGWV6 || defaultIFV6 != m.darwinDefaultIFV6
			}
		}
	}
	m.commitDarwinDefaultRoute(defaultGW, defaultIF, changed)
	if defaultIFV6 != "" {
		m.commitDarwinDefaultRouteV6(defaultGWV6, defaultIFV6, changedV6)
	}
	if err := m.validateDarwinProxyBypass(defaultIF, defaultIFV6); err != nil {
		return err
	}

	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		upstreamGW := defaultGW
		if upstreamUsesIPv6(upstreamIP) {
			upstreamGW = defaultGWV6
		}
		if upstreamGW == "" {
			return fmt.Errorf("ensure darwin upstream route %s: empty default gateway", upstreamIP)
		}
		if changed {
			runCommandIgnore("route", darwinDeleteHostRouteArgs(upstreamIP)...)
		}
		if err := addOrChangeDarwinHostRoute(upstreamIP, upstreamGW); err != nil {
			return fmt.Errorf("ensure darwin upstream route %s: %w", upstreamIP, err)
		}
	}
	for _, splitNet := range []string{"0.0.0.0/1", "128.0.0.0/1"} {
		if err := addOrChangeDarwinSplitRoute(splitNet, m.tunName); err != nil {
			return fmt.Errorf("ensure darwin split route %s: %w", splitNet, err)
		}
	}
	if m.darwinDefaultIFV6 != "" {
		for _, splitNetV6 := range []string{"::/1", "8000::/1"} {
			if err := addOrChangeDarwinSplitRouteV6(splitNetV6, m.tunName); err != nil {
				return fmt.Errorf("ensure darwin split route v6 %s: %w", splitNetV6, err)
			}
		}
	}
	if err := m.refreshDarwinLANBypass(defaultIF, defaultIFV6); err != nil {
		return err
	}
	for _, upstreamIP := range m.effectiveUpstreamIPs() {
		routeOut, err := runCommandOutput("route", darwinRouteGetArgs(upstreamIP)...)
		if err != nil {
			return fmt.Errorf("ensure darwin upstream route %s: %w", upstreamIP, err)
		}
		routeGW, routeIF := parseDarwinDefaultRoute(routeOut)
		if routeIF == m.tunName {
			return fmt.Errorf("ensure darwin upstream route loop detected: host=%s interface=%s", upstreamIP, routeIF)
		}
		if routeGW == "" {
			return fmt.Errorf("ensure darwin upstream route %s: empty gateway", upstreamIP)
		}
		if changed {
			logf(LogLevelDebug, "[DEBUG] [NET] darwin upstream route host=%s | gateway=%s interface=%s", upstreamIP, routeGW, routeIF)
		}
	}
	if err := m.refreshDirectHostRoutesLocked(); err != nil {
		return err
	}

	return nil
}

func (m *AutoRouteManager) commitLinuxDefaultRoute(routeLine, routeGW, routeDev string, changed bool) {
	m.linuxDefaultRoute = routeLine
	m.linuxDefaultGW = routeGW
	m.linuxDefaultDev = routeDev
	if changed {
		logf(LogLevelSystem, "[SYS] [NET] linux default route changed: route=%q via=%s dev=%s", routeLine, routeGW, routeDev)
		m.notifyDefaultRouteChange()
	}
}

func (m *AutoRouteManager) commitDarwinDefaultRoute(defaultGW, defaultIF string, changed bool) {
	m.darwinDefaultGW = defaultGW
	m.darwinDefaultIF = defaultIF
	if changed {
		logf(LogLevelSystem, "[SYS] [NET] darwin default route changed: gateway=%s interface=%s", defaultGW, defaultIF)
		m.notifyDefaultRouteChange()
	}
}

func (m *AutoRouteManager) commitLinuxDefaultRouteV6(routeLine, routeGW, routeDev string, changed bool) {
	m.linuxDefaultRouteV6 = routeLine
	m.linuxDefaultGWV6 = routeGW
	m.linuxDefaultDevV6 = routeDev
	if changed {
		logf(LogLevelSystem, "[SYS] [NET] linux default route v6 changed: route=%q via=%s dev=%s", routeLine, routeGW, routeDev)
		m.notifyDefaultRouteChange()
	}
}

func (m *AutoRouteManager) commitDarwinDefaultRouteV6(defaultGW, defaultIF string, changed bool) {
	m.darwinDefaultGWV6 = defaultGW
	m.darwinDefaultIFV6 = defaultIF
	if changed {
		logf(LogLevelSystem, "[SYS] [NET] darwin default route v6 changed: gateway=%s interface=%s", defaultGW, defaultIF)
		m.notifyDefaultRouteChange()
	}
}

func upstreamUsesIPv6(upstreamIP string) bool {
	ip := net.ParseIP(strings.TrimSpace(upstreamIP))
	return ip != nil && ip.To4() == nil
}

func (m *AutoRouteManager) effectiveUpstreamIPs() []string {
	if len(m.upstreamIPs) > 0 {
		return m.upstreamIPs
	}
	if strings.TrimSpace(m.upstreamIP) == "" {
		return nil
	}
	return []string{m.upstreamIP}
}

func (m *AutoRouteManager) linuxUpstreamRoute(upstreamIP string) (string, string, error) {
	if upstreamUsesIPv6(upstreamIP) {
		if strings.TrimSpace(m.linuxDefaultDevV6) == "" {
			return "", "", fmt.Errorf("linux upstream route %s: missing ipv6 default device", upstreamIP)
		}
		return m.linuxDefaultGWV6, m.linuxDefaultDevV6, nil
	}
	if strings.TrimSpace(m.linuxDefaultDev) == "" {
		return "", "", fmt.Errorf("linux upstream route %s: missing ipv4 default device", upstreamIP)
	}
	return m.linuxDefaultGW, m.linuxDefaultDev, nil
}

func (m *AutoRouteManager) darwinUpstreamRoute(upstreamIP string) (string, string, error) {
	if upstreamUsesIPv6(upstreamIP) {
		if strings.TrimSpace(m.darwinDefaultIFV6) == "" {
			return "", "", fmt.Errorf("darwin upstream route %s: missing ipv6 default interface", upstreamIP)
		}
		if strings.TrimSpace(m.darwinDefaultGWV6) == "" {
			return "", "", fmt.Errorf("darwin upstream route %s: empty ipv6 default gateway", upstreamIP)
		}
		return m.darwinDefaultGWV6, m.darwinDefaultIFV6, nil
	}
	if strings.TrimSpace(m.darwinDefaultIF) == "" {
		return "", "", fmt.Errorf("darwin upstream route %s: missing ipv4 default interface", upstreamIP)
	}
	if strings.TrimSpace(m.darwinDefaultGW) == "" {
		return "", "", fmt.Errorf("darwin upstream route %s: empty ipv4 default gateway", upstreamIP)
	}
	return m.darwinDefaultGW, m.darwinDefaultIF, nil
}

func resolveUpstreamIP(addr string) (string, error) {
	host := strings.TrimSpace(addr)
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "", fmt.Errorf("resolve upstream route host from %q: empty host", addr)
	}

	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", fmt.Errorf("resolve upstream host %q: %w", host, err)
	}
	if resolvedIP, ok := pickResolvedUpstreamIP(addrs); ok {
		return resolvedIP, nil
	}

	return "", fmt.Errorf("resolve upstream host %q: no IP address found", host)
}

func pickResolvedUpstreamIP(addrs []net.IPAddr) (string, bool) {
	for _, a := range addrs {
		if a.IP != nil {
			return a.IP.String(), true
		}
	}
	return "", false
}

func linuxTunCIDR(raw string) (string, error) {
	cidr := strings.TrimSpace(raw)
	if cidr == "" {
		return "198.18.0.1/15", nil
	}
	if _, _, err := net.ParseCIDR(cidr); err != nil {
		return "", fmt.Errorf("invalid tun.ipv4 for linux auto-route (want CIDR): %w", err)
	}
	return cidr, nil
}

func linuxTunIPv6CIDR(raw string) (string, error) {
	cidr := strings.TrimSpace(raw)
	if cidr == "" {
		return "fd00:198:18::1/64", nil
	}
	if !strings.Contains(cidr, "/") {
		ip := net.ParseIP(cidr)
		if ip == nil || ip.To16() == nil || ip.To4() != nil {
			return "", fmt.Errorf("invalid tun.ipv6 for linux auto-route: %q", cidr)
		}
		return ip.String() + "/64", nil
	}
	ip, _, err := net.ParseCIDR(cidr)
	if err != nil || ip == nil || ip.To16() == nil || ip.To4() != nil {
		return "", fmt.Errorf("invalid tun.ipv6 for linux auto-route: %q", cidr)
	}
	return cidr, nil
}

func darwinTunIPMask(raw string) (string, string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "198.18.0.1", "255.254.0.0", nil
	}

	if !strings.Contains(value, "/") {
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() == nil {
			return "", "", fmt.Errorf("invalid tun.ipv4 for darwin auto-route: %q", value)
		}
		return ip.To4().String(), "255.254.0.0", nil
	}

	ip, network, err := net.ParseCIDR(value)
	if err != nil {
		return "", "", fmt.Errorf("invalid tun.ipv4 for darwin auto-route: %w", err)
	}
	ip4 := ip.To4()
	if ip4 == nil || len(network.Mask) != net.IPv4len {
		return "", "", fmt.Errorf("darwin auto-route requires IPv4 tun.ipv4, got %q", value)
	}

	mask := net.IP(network.Mask)
	return ip4.String(), mask.String(), nil
}

func darwinTunIPv6(raw string) (string, int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "fd00:198:18::1", 64, nil
	}
	if !strings.Contains(value, "/") {
		ip := net.ParseIP(value)
		if ip == nil || ip.To16() == nil || ip.To4() != nil {
			return "", 0, fmt.Errorf("invalid tun.ipv6 for darwin auto-route: %q", value)
		}
		return ip.String(), 64, nil
	}
	ip, network, err := net.ParseCIDR(value)
	if err != nil || ip == nil || ip.To16() == nil || ip.To4() != nil {
		return "", 0, fmt.Errorf("invalid tun.ipv6 for darwin auto-route: %q", value)
	}
	ones, _ := network.Mask.Size()
	return ip.String(), ones, nil
}

func parseLinuxDefaultRoute(line string) (string, string) {
	var via, dev string
	fields := strings.Fields(line)
	for i := 0; i+1 < len(fields); i++ {
		switch fields[i] {
		case "via":
			via = fields[i+1]
		case "dev":
			dev = fields[i+1]
		}
	}
	return via, dev
}

func linuxInterfaceCIDRs(dev string) ([]string, error) {
	if strings.TrimSpace(dev) == "" {
		return nil, nil
	}
	outV4, err := runCommandOutput("ip", "-o", "-4", "addr", "show", "dev", dev, "scope", "global")
	if err != nil {
		return nil, err
	}
	outV6, err := runCommandOutput("ip", "-o", "-6", "addr", "show", "dev", dev, "scope", "global")
	if err != nil {
		return nil, err
	}
	cidrs := parseLinuxInterfaceCIDRs(outV4)
	cidrs = append(cidrs, parseLinuxInterfaceCIDRs(outV6)...)
	return dedupeStrings(cidrs), nil
}

func parseLinuxInterfaceCIDRs(out string) []string {
	var cidrs []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] != "inet" && fields[i] != "inet6" {
				continue
			}
			cidr := strings.TrimSpace(fields[i+1])
			ip, network, err := net.ParseCIDR(cidr)
			if err != nil || ip == nil {
				continue
			}
			if ip.IsLoopback() {
				continue
			}
			if ip.To4() != nil {
				if ip.IsLinkLocalUnicast() {
					continue
				}
				ones, bits := network.Mask.Size()
				if bits != 32 {
					continue
				}
				cidrs = append(cidrs, fmt.Sprintf("%s/%d", ip.Mask(network.Mask).String(), ones))
				continue
			}
			ones, bits := network.Mask.Size()
			if bits != 128 {
				continue
			}
			cidrs = append(cidrs, fmt.Sprintf("%s/%d", ip.Mask(network.Mask).String(), ones))
		}
	}
	return dedupeStrings(cidrs)
}

func selectLinuxBypassRoute(out, tunName string) (string, string, string) {
	bestLine := ""
	bestVia := ""
	bestDev := ""
	bestMetric := int(^uint(0) >> 1)
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		via, dev := parseLinuxDefaultRoute(trimmed)
		if dev == "" || dev == tunName {
			continue
		}
		metric, ok := parseLinuxRouteMetric(trimmed)
		if !ok {
			metric = 0 /* Linux omits the field for the default priority; prefer it over larger explicit metrics. */
		}
		if bestLine == "" || metric < bestMetric {
			bestLine = trimmed
			bestVia = via
			bestDev = dev
			bestMetric = metric
		}
	}
	if bestLine == "" {
		return "", "", ""
	}
	return bestLine, bestVia, bestDev
}

func parseLinuxRouteMetric(line string) (int, bool) {
	fields := strings.Fields(strings.TrimSpace(line))
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] != "metric" {
			continue
		}
		metric, err := strconv.Atoi(fields[i+1])
		if err != nil {
			return 0, false
		}
		return metric, true
	}
	return 0, false
}

func parseDarwinDefaultRoute(out string) (string, string) {
	var gateway, iface string
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "gateway:") {
			gateway = strings.TrimSpace(strings.TrimPrefix(trimmed, "gateway:"))
		}
		if strings.HasPrefix(trimmed, "interface:") {
			iface = strings.TrimSpace(strings.TrimPrefix(trimmed, "interface:"))
		}
	}
	return gateway, iface
}

func routeCIDRProbeIP(cidr string) (string, error) {
	ip, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
	if err != nil {
		return "", err
	}
	ip4 := ip.To4()
	if ip4 != nil {
		networkIP := ip4.Mask(network.Mask)
		ones, bits := network.Mask.Size()
		if bits != 32 {
			return "", fmt.Errorf("non-ipv4 cidr %q", cidr)
		}
		if ones >= 31 {
			return networkIP.String(), nil
		}
		probe := append(net.IP(nil), networkIP...)
		probe[3]++
		if !network.Contains(probe) || probe.Equal(networkIP) {
			return "", fmt.Errorf("cannot derive probe ip for %q", cidr)
		}
		return probe.String(), nil
	}
	if ip.To16() == nil {
		return "", fmt.Errorf("invalid cidr %q", cidr)
	}
	networkIP := ip.Mask(network.Mask)
	ones, bits := network.Mask.Size()
	if bits != 128 {
		return "", fmt.Errorf("non-ipv6 cidr %q", cidr)
	}
	if ones >= 127 {
		return networkIP.String(), nil
	}
	probe := append(net.IP(nil), networkIP...)
	for i := len(probe) - 1; i >= 0; i-- {
		probe[i]++
		if probe[i] != 0 {
			break
		}
	}
	if !network.Contains(probe) || probe.Equal(networkIP) {
		return "", fmt.Errorf("cannot derive probe ip for %q", cidr)
	}
	return probe.String(), nil
}

func linuxRouteGetDevice(out string) string {
	fields := strings.Fields(strings.TrimSpace(out))
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "dev" {
			return fields[i+1]
		}
	}
	return ""
}

func linuxRouteGetArgs(target string) []string {
	args := []string{"ip"}
	if ip := net.ParseIP(strings.TrimSpace(target)); ip != nil && ip.To4() == nil {
		args = append(args, "-6")
	}
	args = append(args, "route", "get", target)
	return args
}

func replaceLinuxInterfaceRoute(cidr, dev string) error {
	args := []string{"ip"}
	if isIPv6CIDR(cidr) {
		args = append(args, "-6")
	}
	args = append(args, "route", "replace", cidr, "dev", dev)
	if !isIPv6CIDR(cidr) {
		args = append(args, "scope", "link")
	}
	return runCommand(args[0], args[1:]...)
}

func replaceLinuxCIDRRoute(cidr, via, dev string) error {
	if strings.TrimSpace(cidr) == "" {
		return errors.New("replace linux cidr route: empty cidr")
	}
	if strings.TrimSpace(dev) == "" {
		return fmt.Errorf("replace linux cidr route %s: empty device", cidr)
	}
	args := []string{"ip"}
	if isIPv6CIDR(cidr) {
		args = append(args, "-6")
	}
	args = append(args, "route", "replace", cidr)
	if strings.TrimSpace(via) != "" {
		args = append(args, "via", via)
	}
	args = append(args, "dev", dev)
	if err := runCommand(args[0], args[1:]...); err != nil {
		if strings.TrimSpace(via) == "" || !isLinuxInvalidGatewayError(err) {
			return err
		}
		retryArgs := append(append([]string(nil), args...), "onlink")
		if retryErr := runCommand(retryArgs[0], retryArgs[1:]...); retryErr != nil {
			return errors.Join(err, retryErr)
		}
	}
	return nil
}

func linuxDeleteRouteArgs(cidr, dev string) []string {
	args := []string{"ip"}
	if isIPv6CIDR(cidr) {
		args = append(args, "-6")
	}
	args = append(args, "route", "del", cidr)
	if dev != "" {
		args = append(args, "dev", dev)
	}
	return args
}

func darwinRouteGetArgs(target string) []string {
	args := []string{"-n", "get"}
	if ip := net.ParseIP(strings.TrimSpace(target)); ip != nil && ip.To4() == nil {
		args = append(args, "-inet6")
	}
	args = append(args, target)
	return args
}

func darwinInterfaceCIDRs(iface string) ([]string, error) {
	if strings.TrimSpace(iface) == "" {
		return nil, nil
	}
	out, err := runCommandOutput("ifconfig", iface)
	if err != nil {
		return nil, err
	}
	return parseDarwinInterfaceCIDRs(out), nil
}

func parseDarwinInterfaceCIDRs(out string) []string {
	var cidrs []string
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "inet":
			ip := net.ParseIP(fields[1]).To4()
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			maskHex := ""
			for i := 2; i+1 < len(fields); i++ {
				if fields[i] == "netmask" {
					maskHex = fields[i+1]
					break
				}
			}
			mask, ok := parseDarwinHexNetmask(maskHex)
			if !ok {
				continue
			}
			networkIP := ip.Mask(mask)
			ones, bits := mask.Size()
			if bits != 32 {
				continue
			}
			cidrs = append(cidrs, fmt.Sprintf("%s/%d", networkIP.String(), ones))
		case "inet6":
			ip := net.ParseIP(strings.Split(fields[1], "%")[0])
			if ip == nil || ip.To4() != nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}
			prefixLen := 64
			for i := 2; i+1 < len(fields); i++ {
				if fields[i] == "prefixlen" {
					parsed, err := strconv.Atoi(fields[i+1])
					if err == nil {
						prefixLen = parsed
					}
					break
				}
			}
			mask := net.CIDRMask(prefixLen, 128)
			networkIP := ip.Mask(mask)
			cidrs = append(cidrs, fmt.Sprintf("%s/%d", networkIP.String(), prefixLen))
		}
	}
	return dedupeStrings(cidrs)
}

func parseDarwinHexNetmask(raw string) (net.IPMask, bool) {
	value := strings.TrimSpace(strings.ToLower(raw))
	value = strings.TrimPrefix(value, "0x")
	if len(value) != 8 {
		return nil, false
	}
	mask := make(net.IPMask, 4)
	for i := 0; i < 4; i++ {
		b, err := parseHexByte(value[i*2 : i*2+2])
		if err != nil {
			return nil, false
		}
		mask[i] = b
	}
	return mask, true
}

func parseHexByte(raw string) (byte, error) {
	var v byte
	for i := 0; i < len(raw); i++ {
		v <<= 4
		switch c := raw[i]; {
		case c >= '0' && c <= '9':
			v |= c - '0'
		case c >= 'a' && c <= 'f':
			v |= c - 'a' + 10
		default:
			return 0, fmt.Errorf("invalid hex byte %q", raw)
		}
	}
	return v, nil
}

func addOrChangeDarwinHostRoute(host, gateway string) error {
	addArgs := darwinHostRouteArgs("add", host, gateway)
	changeArgs := darwinHostRouteArgs("change", host, gateway)
	if err := runCommand("route", addArgs...); err != nil {
		if err2 := runCommand("route", changeArgs...); err2 != nil {
			runCommandIgnore("route", darwinDeleteHostRouteArgs(host)...)
			if err3 := runCommand("route", addArgs...); err3 != nil {
				return errors.Join(err, err2, err3)
			}
		}
	}
	return nil
}

func darwinHostRouteArgs(verb, host, gateway string) []string {
	args := []string{"-n", verb}
	if ip := net.ParseIP(strings.TrimSpace(host)); ip != nil && ip.To4() == nil {
		args = append(args, "-inet6")
	}
	args = append(args, "-host", host, gateway)
	return args
}

func darwinDeleteHostRouteArgs(host string) []string {
	args := []string{"-n", "delete"}
	if ip := net.ParseIP(strings.TrimSpace(host)); ip != nil && ip.To4() == nil {
		args = append(args, "-inet6")
	}
	args = append(args, "-host", host)
	return args
}

func darwinDeleteNetRouteArgs(cidr string) []string {
	args := []string{"-n", "delete"}
	if isIPv6CIDR(cidr) {
		args = append(args, "-inet6")
	}
	args = append(args, "-net", cidr)
	return args
}

func addOrChangeDarwinInterfaceRoute(cidr, iface string) error {
	addArgs := darwinInterfaceRouteArgs("add", cidr, iface)
	changeArgs := darwinInterfaceRouteArgs("change", cidr, iface)
	if err := runCommand("route", addArgs...); err != nil {
		if err2 := runCommand("route", changeArgs...); err2 != nil {
			runCommandIgnore("route", darwinDeleteNetRouteArgs(cidr)...)
			if err3 := runCommand("route", addArgs...); err3 != nil {
				return errors.Join(err, err2, err3)
			}
		}
	}
	return nil
}

func addOrChangeDarwinCIDRRoute(cidr, gateway string) error {
	addArgs := darwinCIDRRouteArgs("add", cidr, gateway)
	changeArgs := darwinCIDRRouteArgs("change", cidr, gateway)
	if err := runCommand("route", addArgs...); err != nil {
		if err2 := runCommand("route", changeArgs...); err2 != nil {
			runCommandIgnore("route", darwinDeleteNetRouteArgs(cidr)...)
			if err3 := runCommand("route", addArgs...); err3 != nil {
				return errors.Join(err, err2, err3)
			}
		}
	}
	return nil
}

func darwinCIDRRouteArgs(verb, cidr, gateway string) []string {
	args := []string{"-n", verb}
	if isIPv6CIDR(cidr) {
		args = append(args, "-inet6")
	}
	args = append(args, "-net", cidr, gateway)
	return args
}

func darwinInterfaceRouteArgs(verb, cidr, iface string) []string {
	args := []string{"-n", verb}
	if isIPv6CIDR(cidr) {
		args = append(args, "-inet6")
	}
	args = append(args, "-net", cidr, "-interface", iface)
	return args
}

func addOrChangeDarwinSplitRoute(cidr, tunName string) error {
	addArgs := []string{"-n", "add", "-net", cidr, "-interface", tunName}
	changeArgs := []string{"-n", "change", "-net", cidr, "-interface", tunName}
	if err := runCommand("route", addArgs...); err != nil {
		if err2 := runCommand("route", changeArgs...); err2 != nil {
			return errors.Join(err, err2)
		}
	}
	return nil
}

func addOrChangeDarwinSplitRouteV6(cidr, tunName string) error {
	addArgs := []string{"-n", "add", "-inet6", "-net", cidr, "-interface", tunName}
	changeArgs := []string{"-n", "change", "-inet6", "-net", cidr, "-interface", tunName}
	if err := runCommand("route", addArgs...); err != nil {
		if err2 := runCommand("route", changeArgs...); err2 != nil {
			return errors.Join(err, err2)
		}
	}
	return nil
}

func firstNonEmptyLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func isIPv6CIDR(cidr string) bool {
	ip, _, err := net.ParseCIDR(strings.TrimSpace(cidr))
	return err == nil && ip != nil && ip.To4() == nil && ip.To16() != nil
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func diffCIDRSets(current, desired []string) (remove []string, add []string) {
	currentSet := make(map[string]struct{}, len(current))
	desiredSet := make(map[string]struct{}, len(desired))
	for _, cidr := range current {
		currentSet[cidr] = struct{}{}
	}
	for _, cidr := range desired {
		desiredSet[cidr] = struct{}{}
		if _, ok := currentSet[cidr]; !ok {
			add = append(add, cidr)
		}
	}
	for _, cidr := range current {
		if _, ok := desiredSet[cidr]; !ok {
			remove = append(remove, cidr)
		}
	}
	return remove, add
}

func bypassCIDRsForFamily(currentLAN []string, ipv6 bool) []string {
	combined := make([]string, 0, len(currentLAN)+len(autoRoutePrivateBypassCIDRs))
	combined = append(combined, currentLAN...)
	for _, cidr := range autoRoutePrivateBypassCIDRs {
		if isIPv6CIDR(cidr) == ipv6 {
			combined = append(combined, cidr)
		}
	}
	return dedupeStrings(combined)
}

func validateProxyCIDRsAgainstAutoBypass(proxyCIDRs []*net.IPNet, bypassCIDRs []string) error {
	if len(proxyCIDRs) == 0 || len(bypassCIDRs) == 0 {
		return nil
	}
	for _, bypassValue := range bypassCIDRs {
		_, bypass, err := net.ParseCIDR(strings.TrimSpace(bypassValue))
		if err != nil {
			return fmt.Errorf("invalid auto-route bypass CIDR %q: %w", bypassValue, err)
		}
		for _, proxy := range proxyCIDRs {
			if cidrNetworksOverlap(proxy, bypass) {
				return &autoRouteProxyBypassConflictError{
					proxyCIDR:  proxy.String(),
					bypassCIDR: bypass.String(),
				}
			}
		}
	}
	return nil
}

func cidrNetworksOverlap(left, right *net.IPNet) bool {
	if left == nil || right == nil {
		return false
	}
	leftIP := normalizeIP(left.IP)
	rightIP := normalizeIP(right.IP)
	if leftIP == nil || rightIP == nil || (leftIP.To4() == nil) != (rightIP.To4() == nil) {
		return false
	}
	return left.Contains(rightIP) || right.Contains(leftIP)
}

func familyCIDRs(cidrs []string, ipv6 bool) []string {
	var out []string
	for _, cidr := range cidrs {
		if isIPv6CIDR(cidr) == ipv6 {
			out = append(out, cidr)
		}
	}
	return out
}

func (m *AutoRouteManager) ensureDirectHostRouteLocked(ip net.IP) error {
	host := ip.String()
	cidr, state, ok := m.directHostRouteStateLocked(ip)
	if !ok {
		return nil
	}
	if m.installedDirectHosts == nil {
		m.installedDirectHosts = make(map[string]directCIDRRouteState)
	}
	if previous, ok := m.installedDirectHosts[host]; ok && previous == state {
		m.touchDirectHostRouteLocked(host)
		return nil
	}
	if m.pendingDirectHosts == nil {
		m.pendingDirectHosts = make(map[string]*directHostRoutePending)
	}
	if pending := m.pendingDirectHosts[host]; pending != nil {
		m.mu.Unlock()
		<-pending.done
		m.mu.Lock()
		return pending.err
	}
	if _, ok := m.installedDirectHosts[host]; !ok {
		m.evictDirectHostRoutesLocked(1)
	}
	pending := &directHostRoutePending{done: make(chan struct{})}
	m.pendingDirectHosts[host] = pending
	m.routeWG.Add(1)
	m.mu.Unlock()
	m.commandMu.Lock()
	start := time.Now()
	if err := replaceRouteCIDRViaDefault(cidr, state.gateway, state.dev); err != nil {
		duration := time.Since(start)
		m.commandMu.Unlock()
		pending.err = fmt.Errorf("ensure direct host route %s: %w", host, err)
		logf(LogLevelDebug, "[DEBUG] [NET] direct host route install failed host=%s cidr=%s dev=%s gateway=%s duration=%s err=%v", host, cidr, state.dev, state.gateway, formatDuration(duration), err)
		m.mu.Lock()
		delete(m.pendingDirectHosts, host)
		close(pending.done)
		m.routeWG.Done()
		return pending.err
	}
	duration := time.Since(start)
	m.commandMu.Unlock()
	logf(LogLevelDebug, "[DEBUG] [NET] direct host route installed host=%s cidr=%s dev=%s gateway=%s duration=%s", host, cidr, state.dev, state.gateway, formatDuration(duration))
	m.mu.Lock()
	if m.tearingDown {
		/* The route was installed after teardown began and was never added to the
		   tracked set, so roll it back before allowing Teardown to continue. */
		m.mu.Unlock()
		m.commandMu.Lock()
		rollbackErr := removeHostRoute(host)
		m.commandMu.Unlock()
		m.mu.Lock()
		if rollbackErr != nil {
			pending.err = fmt.Errorf("rollback direct host route %s during teardown: %w", host, rollbackErr)
			m.teardownRouteErr = errors.Join(m.teardownRouteErr, pending.err)
		}
	}
	delete(m.pendingDirectHosts, host)
	if !m.tearingDown {
		m.installedDirectHosts[host] = state
		m.touchDirectHostRouteLocked(host)
	}
	close(pending.done)
	m.routeWG.Done()
	return pending.err
}

func (m *AutoRouteManager) refreshDirectHostRoutesLocked() error {
	for host := range m.installedDirectHosts {
		ip := net.ParseIP(host)
		if ip == nil {
			m.forgetDirectHostRouteLocked(host, false)
			continue
		}
		if m.hostCoveredByBypassLocked(ip) {
			m.forgetDirectHostRouteLocked(host, true)
			continue
		}
		cidr, state, ok := m.directHostRouteStateLocked(ip)
		if !ok {
			continue
		}
		if previous, ok := m.installedDirectHosts[host]; ok && previous == state {
			continue
		}
		if err := replaceRouteCIDRViaDefault(cidr, state.gateway, state.dev); err != nil {
			return fmt.Errorf("ensure direct host route %s: %w", host, err)
		}
		m.installedDirectHosts[host] = state
	}
	return nil
}

func (m *AutoRouteManager) evictDirectHostRoutesLocked(incoming int) {
	if maxDirectHostRoutes <= 0 || incoming <= 0 {
		return
	}
	for len(m.installedDirectHosts)+incoming > maxDirectHostRoutes {
		evicted := false
		for len(m.installedDirectHostOrder) > 0 {
			host := m.installedDirectHostOrder[0]
			m.installedDirectHostOrder = m.installedDirectHostOrder[1:]
			if _, ok := m.installedDirectHosts[host]; !ok {
				continue
			}
			delete(m.installedDirectHosts, host)
			removeHostRouteBestEffort(host)
			evicted = true
			break
		}
		if evicted {
			continue
		}
		for host := range m.installedDirectHosts {
			delete(m.installedDirectHosts, host)
			removeHostRouteBestEffort(host)
			evicted = true
			break
		}
		if !evicted {
			return
		}
	}
}

func (m *AutoRouteManager) touchDirectHostRouteLocked(host string) {
	host = strings.TrimSpace(host)
	if host == "" {
		return
	}
	for i, current := range m.installedDirectHostOrder {
		if current == host {
			copy(m.installedDirectHostOrder[i:], m.installedDirectHostOrder[i+1:])
			m.installedDirectHostOrder = m.installedDirectHostOrder[:len(m.installedDirectHostOrder)-1]
			break
		}
	}
	m.installedDirectHostOrder = append(m.installedDirectHostOrder, host)
}

func (m *AutoRouteManager) forgetDirectHostRouteLocked(host string, removeRoute bool) {
	host = strings.TrimSpace(host)
	if host == "" {
		return
	}
	delete(m.installedDirectHosts, host)
	for i, current := range m.installedDirectHostOrder {
		if current == host {
			copy(m.installedDirectHostOrder[i:], m.installedDirectHostOrder[i+1:])
			m.installedDirectHostOrder = m.installedDirectHostOrder[:len(m.installedDirectHostOrder)-1]
			break
		}
	}
	if removeRoute {
		removeHostRouteBestEffort(host)
	}
}

func (m *AutoRouteManager) directHostRouteStateLocked(ip net.IP) (string, directCIDRRouteState, bool) {
	ip = normalizeIP(ip)
	if ip == nil {
		return "", directCIDRRouteState{}, false
	}
	gateway := strings.TrimSpace(m.linuxDefaultGW)
	dev := strings.TrimSpace(m.linuxDefaultDev)
	if runtime.GOOS == "darwin" {
		gateway = strings.TrimSpace(m.darwinDefaultGW)
		dev = strings.TrimSpace(m.darwinDefaultIF)
	}
	cidr := ip.String() + "/32"
	if ip.To4() == nil {
		cidr = ip.String() + "/128"
		gateway = strings.TrimSpace(m.linuxDefaultGWV6)
		dev = strings.TrimSpace(m.linuxDefaultDevV6)
		if runtime.GOOS == "darwin" {
			gateway = strings.TrimSpace(m.darwinDefaultGWV6)
			dev = strings.TrimSpace(m.darwinDefaultIFV6)
		}
	}
	if dev == "" {
		return "", directCIDRRouteState{}, false
	}
	return cidr, directCIDRRouteState{gateway: gateway, dev: dev}, true
}

func (m *AutoRouteManager) hostCoveredByBypassLocked(ip net.IP) bool {
	if ip == nil {
		return false
	}
	ipv6 := ip.To4() == nil
	for _, cidr := range bypassCIDRsForFamily(m.lanCIDRs, ipv6) {
		_, network, err := net.ParseCIDR(cidr)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

func replaceRouteCIDRViaDefault(cidr, gateway, dev string) error {
	switch runtime.GOOS {
	case "linux":
		return replaceLinuxCIDRRoute(cidr, gateway, dev)
	case "darwin":
		if strings.TrimSpace(gateway) == "" {
			return addOrChangeDarwinInterfaceRoute(cidr, dev)
		}
		return addOrChangeDarwinCIDRRoute(cidr, gateway)
	default:
		return nil
	}
}

func removeHostRoute(host string) error {
	var err error
	switch runtime.GOOS {
	case "linux":
		args := linuxDeleteHostRouteArgs(host)
		if len(args) > 0 {
			err = runCommand(args[0], args[1:]...)
		}
	case "darwin":
		err = runCommand("route", darwinDeleteHostRouteArgs(host)...)
	}
	if err != nil && isHostRouteNotFoundError(err) {
		return nil
	}
	return err
}

func removeHostRouteBestEffort(host string) {
	if err := removeHostRoute(host); err != nil {
		logf(LogLevelDebug, "[DEBUG] [SYS] auto-route ignore host route removal failure: host=%s | err=%v", host, err)
	}
}

func isHostRouteNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return isDarwinRouteNotFoundText(msg) || strings.Contains(msg, "no such process")
}

func desiredDarwinLANBypassRoutes(ipv4IF, ipv6IF string) (map[string]string, []string, error) {
	desired := make(map[string]string)
	var lanCIDRs []string
	if strings.TrimSpace(ipv4IF) != "" {
		currentLAN, err := darwinInterfaceCIDRs(ipv4IF)
		if err != nil {
			return nil, nil, fmt.Errorf("discover darwin lan subnets on %s: %w", ipv4IF, err)
		}
		lanCIDRs = append(lanCIDRs, currentLAN...)
		for _, cidr := range bypassCIDRsForFamily(familyCIDRs(currentLAN, false), false) {
			desired[cidr] = ipv4IF
		}
	}
	if strings.TrimSpace(ipv6IF) != "" {
		currentLAN, err := darwinInterfaceCIDRs(ipv6IF)
		if err != nil {
			return nil, nil, fmt.Errorf("discover darwin lan subnets on %s: %w", ipv6IF, err)
		}
		lanCIDRs = append(lanCIDRs, currentLAN...)
		for _, cidr := range bypassCIDRsForFamily(familyCIDRs(currentLAN, true), true) {
			desired[cidr] = ipv6IF
		}
	}
	return desired, dedupeStrings(lanCIDRs), nil
}

func (m *AutoRouteManager) validateDarwinProxyBypass(ipv4IF, ipv6IF string) error {
	desired, _, err := desiredDarwinLANBypassRoutes(ipv4IF, ipv6IF)
	if err != nil {
		return err
	}
	return validateProxyCIDRsAgainstAutoBypass(m.proxyCIDRs, sortedMapKeys(desired))
}

func (m *AutoRouteManager) refreshDarwinLANBypass(ipv4IF, ipv6IF string) error {
	desired, lanCIDRs, err := desiredDarwinLANBypassRoutes(ipv4IF, ipv6IF)
	if err != nil {
		return err
	}
	if err := validateProxyCIDRsAgainstAutoBypass(m.proxyCIDRs, sortedMapKeys(desired)); err != nil {
		return err
	}
	for cidr, iface := range desired {
		probe, err := routeCIDRProbeIP(cidr)
		if err != nil {
			continue
		}
		routeOut, err := runCommandOutput("route", darwinRouteGetArgs(probe)...)
		routeIF := ""
		if err == nil {
			_, routeIF = parseDarwinDefaultRoute(routeOut)
		}
		if routeIF == iface {
			continue
		}
		if err := addOrChangeDarwinInterfaceRoute(cidr, iface); err != nil {
			return fmt.Errorf("ensure darwin lan bypass route %s via %s: %w", cidr, iface, err)
		}
		routeIF, err = darwinResolvedInterfaceForCIDR(cidr)
		if err != nil {
			continue
		}
		if routeIF == iface {
			continue
		}
		runCommandIgnore("route", darwinDeleteNetRouteArgs(cidr)...)
		if err := addOrChangeDarwinInterfaceRoute(cidr, iface); err != nil {
			return fmt.Errorf("repair darwin lan bypass route %s via %s: %w", cidr, iface, err)
		}
		routeIF, err = darwinResolvedInterfaceForCIDR(cidr)
		if err == nil && routeIF != iface {
			return fmt.Errorf("darwin lan bypass route still resolves via %s for %s (want %s)", routeIF, cidr, iface)
		}
	}

	for cidr := range m.installedLANIFs {
		if _, ok := desired[cidr]; !ok {
			m.removeDarwinManagedRoute(cidr)
		}
	}
	m.lanCIDRs = lanCIDRs
	m.installedLANIFs = desired
	m.installedLANCIDRs = sortedMapKeys(desired)
	return nil
}

func darwinResolvedInterfaceForCIDR(cidr string) (string, error) {
	probe, err := routeCIDRProbeIP(cidr)
	if err != nil {
		return "", err
	}
	routeOut, err := runCommandOutput("route", darwinRouteGetArgs(probe)...)
	if err != nil {
		return "", err
	}
	_, routeIF := parseDarwinDefaultRoute(routeOut)
	return routeIF, nil
}

func desiredLinuxLANBypassRoutes(ipv4Dev, ipv6Dev string) (map[string]string, []string, error) {
	desired := make(map[string]string)
	var lanCIDRs []string
	if strings.TrimSpace(ipv4Dev) != "" {
		currentLAN, err := linuxInterfaceCIDRs(ipv4Dev)
		if err != nil {
			return nil, nil, fmt.Errorf("discover linux lan subnets on %s: %w", ipv4Dev, err)
		}
		lanCIDRs = append(lanCIDRs, currentLAN...)
		for _, cidr := range bypassCIDRsForFamily(familyCIDRs(currentLAN, false), false) {
			desired[cidr] = ipv4Dev
		}
	}
	if strings.TrimSpace(ipv6Dev) != "" {
		currentLAN, err := linuxInterfaceCIDRs(ipv6Dev)
		if err != nil {
			return nil, nil, fmt.Errorf("discover linux lan subnets on %s: %w", ipv6Dev, err)
		}
		lanCIDRs = append(lanCIDRs, currentLAN...)
		for _, cidr := range bypassCIDRsForFamily(familyCIDRs(currentLAN, true), true) {
			desired[cidr] = ipv6Dev
		}
	}
	return desired, dedupeStrings(lanCIDRs), nil
}

func (m *AutoRouteManager) validateLinuxProxyBypass(ipv4Dev, ipv6Dev string) error {
	desired, _, err := desiredLinuxLANBypassRoutes(ipv4Dev, ipv6Dev)
	if err != nil {
		return err
	}
	return validateProxyCIDRsAgainstAutoBypass(m.proxyCIDRs, sortedMapKeys(desired))
}

func (m *AutoRouteManager) refreshLinuxLANBypass(ipv4Dev, ipv6Dev string) error {
	desired, lanCIDRs, err := desiredLinuxLANBypassRoutes(ipv4Dev, ipv6Dev)
	if err != nil {
		return err
	}
	if err := validateProxyCIDRsAgainstAutoBypass(m.proxyCIDRs, sortedMapKeys(desired)); err != nil {
		return err
	}
	for cidr, dev := range desired {
		probe, err := routeCIDRProbeIP(cidr)
		if err != nil {
			continue
		}
		routeOut, err := runCommandOutputArgs(linuxRouteGetArgs(probe))
		if err == nil && linuxRouteGetDevice(routeOut) == dev {
			continue
		}
		if err := replaceLinuxInterfaceRoute(cidr, dev); err != nil {
			return fmt.Errorf("ensure linux lan bypass route %s via %s: %w", cidr, dev, err)
		}
	}
	for cidr := range m.installedLANDevs {
		if _, ok := desired[cidr]; !ok {
			m.removeLinuxManagedRoute(cidr)
		}
	}
	m.lanCIDRs = lanCIDRs
	m.installedLANDevs = desired
	m.installedLANCIDRs = sortedMapKeys(desired)
	m.installedLANIFs = nil
	return nil
}

func sortedMapKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}

func (m *AutoRouteManager) removeDarwinManagedRoute(cidr string) {
	if strings.TrimSpace(cidr) == "" {
		return
	}
	iface := strings.TrimSpace(m.installedLANIFs[cidr])
	if iface != "" {
		probe, err := routeCIDRProbeIP(cidr)
		if err == nil {
			routeOut, getErr := runCommandOutput("route", darwinRouteGetArgs(probe)...)
			if getErr == nil {
				_, routeIF := parseDarwinDefaultRoute(routeOut)
				if routeIF != iface {
					return
				}
			}
		}
	}
	runCommandIgnore("route", darwinDeleteNetRouteArgs(cidr)...)
}

func (m *AutoRouteManager) removeLinuxManagedRoute(cidr string) {
	if strings.TrimSpace(cidr) == "" {
		return
	}
	dev := strings.TrimSpace(m.installedLANDevs[cidr])
	if dev != "" {
		probe, err := routeCIDRProbeIP(cidr)
		if err == nil {
			routeOut, getErr := runCommandOutputArgs(linuxRouteGetArgs(probe))
			if getErr == nil && linuxRouteGetDevice(routeOut) != dev {
				return
			}
		}
	}
	runCommandIgnoreArgs(linuxDeleteRouteArgs(cidr, dev))
}

func (m *AutoRouteManager) notifyDefaultRouteChange() {
	m.onChangeMu.RLock()
	fn := m.onChange
	m.onChangeMu.RUnlock()
	if fn != nil {
		fn()
	}
}

func snapshotLinuxDefaultRoute() (string, string, string, error) {
	out, err := runCommandOutput("ip", "route", "show", "default")
	if err != nil {
		return "", "", "", fmt.Errorf("snapshot linux default route: %w", err)
	}
	line, via, dev := selectLinuxBypassRoute(out, "")
	if line == "" {
		line = firstNonEmptyLine(out)
		via, dev = parseLinuxDefaultRoute(line)
	}
	if line == "" {
		return "", "", "", errors.New("snapshot linux default route: empty route output")
	}
	if dev == "" {
		return "", "", "", fmt.Errorf("snapshot linux default route: missing device in %q", line)
	}
	return line, via, dev, nil
}

func snapshotLinuxDefaultRouteV6() (string, string, string, error) {
	out, err := runCommandOutput("ip", "-6", "route", "show", "default")
	if err != nil {
		return "", "", "", fmt.Errorf("snapshot linux default route v6: %w", err)
	}
	line, via, dev := selectLinuxBypassRoute(out, "")
	if line == "" {
		return "", "", "", nil
	}
	if dev == "" {
		return "", "", "", fmt.Errorf("snapshot linux default route v6: missing device in %q", line)
	}
	return line, via, dev, nil
}

func replaceLinuxHostRoute(host, via, dev string) error {
	if strings.TrimSpace(host) == "" {
		return errors.New("replace linux host route: empty host")
	}
	if strings.TrimSpace(dev) == "" {
		return fmt.Errorf("replace linux host route %s: empty device", host)
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return fmt.Errorf("replace linux host route: invalid host %q", host)
	}
	args := []string{"ip"}
	target := host + "/32"
	if ip.To4() == nil {
		args = append(args, "-6")
		target = host + "/128"
	}
	args = append(args, "route", "replace", target)
	if strings.TrimSpace(via) != "" {
		args = append(args, "via", via)
	}
	args = append(args, "dev", dev)
	if err := runCommand(args[0], args[1:]...); err != nil {
		if strings.TrimSpace(via) == "" || !isLinuxInvalidGatewayError(err) {
			return err
		}
		retryArgs := append(append([]string(nil), args...), "onlink")
		if retryErr := runCommand(retryArgs[0], retryArgs[1:]...); retryErr != nil {
			return err
		}
		return nil
	}
	return nil
}

func isLinuxInvalidGatewayError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "nexthop has invalid gateway") || strings.Contains(msg, "invalid gateway")
}

func linuxDeleteHostRouteArgs(host string) []string {
	ip := net.ParseIP(strings.TrimSpace(host))
	if ip == nil {
		return []string{"ip", "route", "del", host}
	}
	args := []string{"ip"}
	target := host + "/32"
	if ip.To4() == nil {
		args = append(args, "-6")
		target = host + "/128"
	}
	args = append(args, "route", "del", target)
	return args
}

func snapshotDarwinDefaultRoute() (string, string, error) {
	out, err := runCommandOutput("route", "-n", "get", "default")
	if err != nil {
		return "", "", fmt.Errorf("snapshot darwin default route: %w", err)
	}
	gateway, iface := parseDarwinDefaultRoute(out)
	if iface == "" {
		return "", "", fmt.Errorf("snapshot darwin default route: missing interface in %q", strings.TrimSpace(out))
	}
	return gateway, iface, nil
}

func snapshotDarwinDefaultRouteV6() (string, string, error) {
	out, err := runCommandOutput("route", "-n", "get", "-inet6", "default")
	if err != nil {
		if isDarwinRouteNotFoundError(err) {
			return "", "", nil
		}
		return "", "", fmt.Errorf("snapshot darwin default route v6: %w", err)
	}
	if isDarwinRouteNotFoundText(out) {
		return "", "", nil
	}
	gateway, iface := parseDarwinDefaultRoute(out)
	if iface == "" {
		return "", "", fmt.Errorf("snapshot darwin default route v6: missing interface in %q", strings.TrimSpace(out))
	}
	return gateway, iface, nil
}

func isDarwinRouteNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return isDarwinRouteNotFoundText(msg)
}

func isDarwinRouteNotFoundText(text string) bool {
	msg := strings.ToLower(text)
	return strings.Contains(msg, "not in table") || strings.Contains(msg, "network is unreachable")
}

func runCommandIgnore(name string, args ...string) {
	if err := runCommand(name, args...); err != nil {
		logf(LogLevelDebug, "[DEBUG] [SYS] auto-route ignore command failure: %s %s | err=%v", name, strings.Join(args, " "), err)
	}
}

func runCommandIgnoreArgs(args []string) {
	if len(args) == 0 {
		return
	}
	runCommandIgnore(args[0], args[1:]...)
}

func runCommand(name string, args ...string) error {
	_, err := runCommandOutput(name, args...)
	return err
}

func runCommandOutputArgs(args []string) (string, error) {
	if len(args) == 0 {
		return "", errors.New("empty command")
	}
	return runCommandOutput(args[0], args[1:]...)
}

func runCommandOutput(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), routeCommandTimeout)
	defer cancel()
	cmd := execCommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("run %s %s: timed out after %s", name, strings.Join(args, " "), routeCommandTimeout)
		}
		extra := strings.TrimSpace(string(out))
		if extra != "" {
			return "", fmt.Errorf("run %s %s: %w: %s", name, strings.Join(args, " "), err, extra)
		}
		return "", fmt.Errorf("run %s %s: %w", name, strings.Join(args, " "), err)
	}
	return string(out), nil
}
