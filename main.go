package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Proxy       string    `json:"proxy"`
	Ok          bool      `json:"ok"`
	Latency     float64   `json:"latency_ms"`
	LastChecked time.Time `json:"last_checked"`
	Anonymity   string    `json:"anonymity,omitempty"`
	Type        string    `json:"type,omitempty"`
	Flags       []string  `json:"flags,omitempty"`
	Error       string    `json:"error,omitempty"`
}

type ProxyInfo struct {
	Proxy       string    `json:"proxy"`
	Latency     float64   `json:"latency_ms"`
	LastChecked time.Time `json:"last_checked"`
	Anonymity   string    `json:"anonymity,omitempty"`
	Type        string    `json:"type,omitempty"`
	Flags       []string  `json:"flags,omitempty"`
}

var (
	goodFile        = "good.txt"
	defaultWorkers  = 200
	defaultTimeout  = 10 * time.Second
	recheckInterval = 5 * time.Minute

	verifyQueue   chan string
	queuedMu      sync.Mutex
	queuedProxies = make(map[string]struct{})

	goodMu  sync.RWMutex
	goodSet = make(map[string]struct{})
	goodMap = make(map[string]ProxyInfo)
	ownIP   string
)

func main() {
	workers := flag.Int("workers", defaultWorkers, "concurrent proxy check workers")
	timeoutSec := flag.Int("timeout", int(defaultTimeout.Seconds()), "proxy test timeout seconds")
	goodFilePath := flag.String("good-file", goodFile, "path to persistent good proxy file")
	flag.Parse()

	goodFile = *goodFilePath
	defaultWorkers = *workers
	defaultTimeout = time.Duration(*timeoutSec) * time.Second
	verifyQueue = make(chan string, 10000)

	loadGoodFile()
	ownIP = fetchOwnIP()
	if ownIP != "" {
		log.Printf("local public IP: %s", ownIP)
	}

	go startVerifierWorkerPool(defaultWorkers)
	go startGoodRechecker()

	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/good", goodHandler)
	http.HandleFunc("/check", checkHandler)
	http.HandleFunc("/elite", eliteHandler)
	http.HandleFunc("/status", statusHandler)

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}

	log.Printf("starting proxy checker on %s with %d workers and %s timeout", addr, defaultWorkers, defaultTimeout)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func loadGoodFile() {
	data, err := os.ReadFile(goodFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("failed to load good file: %v", err)
		return
	}

	lines := strings.Split(string(data), "\n")
	goodMu.Lock()
	defer goodMu.Unlock()
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if _, ok := goodSet[line]; ok {
			continue
		}
		goodSet[line] = struct{}{}
		goodMap[line] = ProxyInfo{Proxy: line}
		queueProxy(line)
	}
}

func startGoodRechecker() {
	ticker := time.NewTicker(recheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		requeueKnownProxies()
	}
}

func requeueKnownProxies() {
	goodMu.RLock()
	proxies := make([]string, 0, len(goodMap))
	for _, info := range goodMap {
		proxies = append(proxies, info.Proxy)
	}
	goodMu.RUnlock()

	for _, proxy := range proxies {
		queueProxy(proxy)
	}
}

func queueProxy(proxy string) {
	proxy = strings.TrimSpace(proxy)
	if proxy == "" {
		return
	}

	queuedMu.Lock()
	if _, ok := queuedProxies[proxy]; ok {
		queuedMu.Unlock()
		return
	}
	queuedProxies[proxy] = struct{}{}
	queuedMu.Unlock()

	verifyQueue <- proxy
}

func startVerifierWorkerPool(count int) {
	for i := 0; i < count; i++ {
		go func() {
			for proxy := range verifyQueue {
				res := testProxy(proxy, defaultTimeout)
				queuedMu.Lock()
				delete(queuedProxies, proxy)
				queuedMu.Unlock()
				if res.Ok {
					saveGood(proxy, res)
				}
			}
		}()
	}
}

func goodHandler(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(goodFile)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "failed to read good file", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Write(data)
}

func checkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	proxies := make([]string, 0, 1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		proxies = append(proxies, line)
	}

	if len(proxies) == 0 {
		http.Error(w, "no proxies provided", http.StatusBadRequest)
		return
	}

	q := defaultWorkers
	if v := r.URL.Query().Get("c"); v != "" {
		var qv int
		fmt.Sscanf(v, "%d", &qv)
		if qv > 0 {
			q = qv
		}
	}

	t := defaultTimeout
	if v := r.URL.Query().Get("t"); v != "" {
		var tv int
		fmt.Sscanf(v, "%d", &tv)
		if tv > 0 {
			t = time.Duration(tv) * time.Second
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	jobs := make(chan string)
	results := make(chan Result)

	var wg sync.WaitGroup
	for i := 0; i < q; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				results <- testProxy(p, t)
			}
		}()
	}

	go func() {
		for _, p := range proxies {
			select {
			case jobs <- p:
			case <-r.Context().Done():
				close(jobs)
				return
			}
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	enc := json.NewEncoder(w)
	for res := range results {
		fmt.Fprintf(w, "data: ")
		if err := enc.Encode(res); err != nil {
			log.Printf("encode error: %v", err)
			break
		}
		fmt.Fprint(w, "\n")
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		default:
		}
	}
}

func eliteHandler(w http.ResponseWriter, r *http.Request) {
	goodMu.RLock()
	proxies := make([]ProxyInfo, 0, len(goodMap))
	for _, info := range goodMap {
		if info.Anonymity == "elite" {
			proxies = append(proxies, info)
		}
	}
	goodMu.RUnlock()

	sort.Slice(proxies, func(i, j int) bool {
		return proxies[i].Latency < proxies[j].Latency
	})

	data, err := json.MarshalIndent(proxies, "", "  ")
	if err != nil {
		http.Error(w, "failed to marshal elite proxies", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	goodMu.RLock()
	status := map[string]any{
		"good_count": len(goodMap),
		"workers":    defaultWorkers,
		"timeout":    defaultTimeout.String(),
		"own_ip":     ownIP,
	}
	goodMu.RUnlock()

	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		http.Error(w, "failed to marshal status", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func testProxy(proxy string, timeout time.Duration) Result {
	res := Result{Proxy: proxy, LastChecked: time.Now(), Anonymity: "unknown", Type: "http"}

	u := proxy
	if !strings.Contains(proxy, "://") {
		u = "http://" + proxy
	}
	proxyURL, err := url.Parse(u)
	if err != nil {
		res.Error = "invalid proxy URL"
		return res
	}

	res.Type = detectProxyType(proxyURL)

	host := proxyURL.Host
	if !strings.Contains(host, ":") {
		host = host + ":80"
	}
	dialTimeout := timeout / 2
	conn, err := net.DialTimeout("tcp", host, dialTimeout)
	if err != nil {
		res.Ok = false
		res.Error = "tcp dial: " + err.Error()
		return res
	}
	conn.Close()

	transport := &http.Transport{
		Proxy:               http.ProxyURL(proxyURL),
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		TLSHandshakeTimeout: dialTimeout,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	endpoints := []string{
		"https://httpbin.org/ip",
		"https://ifconfig.co/ip",
		"https://icanhazip.com/",
		"https://api.ipify.org",
	}

	var lastErr string
	for _, ep := range endpoints {
		u, _ := url.Parse(ep)
		if u.Scheme == "https" {
			host := u.Host
			if !strings.Contains(host, ":") {
				host = host + ":443"
			}
			if err := connectViaProxy(proxyURL, host, timeout/2); err != nil {
				lastErr = err.Error()
				continue
			}
		}

		for a := 0; a < 2; a++ {
			start := time.Now()
			resp, err := client.Get(ep)
			res.Latency = time.Since(start).Seconds() * 1000
			if err != nil {
				lastErr = err.Error()
				time.Sleep(time.Duration(a+1) * 150 * time.Millisecond)
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				res.Ok = true
				res.Flags = append(res.Flags, "reachable")
				if res.Type == "https" {
					res.Flags = append(res.Flags, "secure-proxy")
				}
				if res.Type == "http" {
					res.Flags = append(res.Flags, "legacy-proxy")
				}
				anonymity, err := detectAnonymity(client)
				if err != nil {
					log.Printf("anonymity check failed for %s: %v", proxy, err)
				} else {
					res.Anonymity = anonymity
				}
				saveGood(proxy, res)
				return res
			}
			lastErr = fmt.Sprintf("status %d", resp.StatusCode)
			break
		}
	}

	res.Ok = false
	res.Error = lastErr
	return res
}

func detectProxyType(proxyURL *url.URL) string {
	scheme := strings.ToLower(proxyURL.Scheme)
	if scheme == "" {
		return "http"
	}
	if scheme == "https" || scheme == "http" {
		return scheme
	}
	return "unknown"
}

func detectAnonymity(client *http.Client) (string, error) {
	req, err := http.NewRequest("GET", "https://httpbin.org/get", nil)
	if err != nil {
		return "unknown", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "unknown", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return "unknown", fmt.Errorf("headers endpoint status %d", resp.StatusCode)
	}

	var payload struct {
		Headers map[string]string `json:"headers"`
		Origin  string            `json:"origin"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "unknown", err
	}

	return classifyAnonymity(payload.Headers, payload.Origin), nil
}

func classifyAnonymity(headers map[string]string, origin string) string {
	normalized := make(map[string]string, len(headers))
	for key, value := range headers {
		normalized[strings.ToLower(key)] = strings.TrimSpace(value)
	}

	transparentHeaders := []string{"x-forwarded-for", "forwarded", "x-real-ip", "client-ip"}
	proxyHeaders := []string{"via", "proxy-connection", "x-proxy-id"}

	for _, header := range transparentHeaders {
		if v, ok := normalized[header]; ok && v != "" {
			if ownIP != "" && strings.Contains(v, ownIP) {
				return "transparent"
			}
			return "anonymous"
		}
	}

	for _, header := range proxyHeaders {
		if v, ok := normalized[header]; ok && v != "" {
			return "anonymous"
		}
	}

	return "elite"
}

func fetchOwnIP() string {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("https://api.ipify.org?format=text")
	if err != nil {
		log.Printf("unable to determine local public IP: %v", err)
		return ""
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("unable to read local IP response: %v", err)
		return ""
	}
	return strings.TrimSpace(string(data))
}

func saveGood(proxy string, info Result) {
	info.LastChecked = time.Now()
	goodMu.Lock()
	defer goodMu.Unlock()
	if _, ok := goodSet[proxy]; ok {
		existing := goodMap[proxy]
		existing.Latency = info.Latency
		existing.LastChecked = info.LastChecked
		if info.Anonymity != "" {
			existing.Anonymity = info.Anonymity
		}
		if info.Type != "" {
			existing.Type = info.Type
		}
		if len(info.Flags) > 0 {
			existing.Flags = info.Flags
		}
		goodMap[proxy] = existing
		return
	}
	goodSet[proxy] = struct{}{}
	goodMap[proxy] = ProxyInfo{
		Proxy:       proxy,
		Latency:     info.Latency,
		LastChecked: info.LastChecked,
		Anonymity:   info.Anonymity,
		Type:        info.Type,
		Flags:       info.Flags,
	}

	f, err := os.OpenFile(goodFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("failed to open good file: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.WriteString(proxy + "\n"); err != nil {
		log.Printf("failed to write good file: %v", err)
	}
}

func connectViaProxy(proxyURL *url.URL, targetHost string, timeout time.Duration) error {
	proxyHost := proxyURL.Host
	if !strings.Contains(proxyHost, ":") {
		proxyHost = proxyHost + ":80"
	}
	conn, err := net.DialTimeout("tcp", proxyHost, timeout)
	if err != nil {
		return fmt.Errorf("tcp dial: %w", err)
	}

	if proxyURL.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		conn = net.Conn(tlsConn)
		if err := tlsConn.SetDeadline(time.Now().Add(timeout)); err != nil {
			// ignore deadline error
		}
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return fmt.Errorf("tls handshake to proxy: %w", err)
		}
	}

	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n\r\n", targetHost, targetHost)
	if _, err := conn.Write([]byte(req)); err != nil {
		return fmt.Errorf("connect write: %w", err)
	}

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("connect read: %w", err)
	}
	if !strings.Contains(statusLine, "200") {
		for {
			line, err := br.ReadString('\n')
			if err != nil || line == "\r\n" || line == "\n" {
				break
			}
		}
		return fmt.Errorf("proxy connect failed: %s", strings.TrimSpace(statusLine))
	}
	return nil
}
