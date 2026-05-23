# Ultra Proxy Checker

 Simple single-page Go-based proxy checker that streams results as they complete.
 
 Good proxies are appended to `good.txt` and can be downloaded from the UI ("Download Good List").
 The checker uses multiple public endpoints (httpbin, ifconfig.co, icanhazip, api.ipify) and a TCP pre-check to reduce reliance on a single verification source.
 
 Caveats and recommendations:
 Public services like `httpbin.org` and others have rate limits and may block heavy requests. For large-scale checks (5000+ proxies) you should self-host a lightweight endpoint (for example a simple `/ip` that returns remote IP) or distribute checks across multiple endpoints.
 Consider staggering batches, lowering concurrency, or adding retries/backoff to avoid being blocked.
 SOCKS5 proxy checks and HTTPS-specific behaviors may need additional transport configuration.

Run:

```bash
cd /home/harshitsingh/proxy-project-2
go run .
```

Open http://localhost:8080 and paste proxies (one per line) like `1.2.3.4:8080`.

Notes:
- Uses HTTP(S) proxies (prefix with `http://` or `https://` if needed). If the proxy has no scheme `http://` is assumed.
- Tune concurrency with the Concurrency input (default 200) and timeout in seconds.
- The server streams JSON results to the page as they finish.
