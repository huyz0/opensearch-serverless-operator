// s3-fault-proxy is a disposable test proxy for the writer retirement drill.
package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"
)

const modePath = "/tmp/s3-fault-mode"

func main() {
	if len(os.Args) == 3 && os.Args[1] == "mode" {
		if err := setMode(os.Args[2]); err != nil {
			log.Fatal(err)
		}
		return
	}
	upstream, err := url.Parse("http://s3-backend:8333")
	if err != nil {
		log.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	server := &http.Server{
		Addr:              ":8333",
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mode, err := os.ReadFile(modePath)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				http.Error(w, "cannot read fixture mode", http.StatusInternalServerError)
				return
			}
			switch string(mode) {
			case "throttle":
				logFault(r, "SlowDown")
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("<Error><Code>SlowDown</Code><Message>Reduce your request rate</Message></Error>"))
			case "disconnect":
				logFault(r, "Disconnect")
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					http.Error(w, "fixture could not disconnect", http.StatusInternalServerError)
					return
				}
				_ = conn.Close()
			default:
				proxy.ServeHTTP(w, r)
			}
		}),
	}
	log.Fatal(server.ListenAndServe())
}

func setMode(mode string) error {
	switch mode {
	case "healthy", "throttle", "disconnect":
	default:
		return errors.New("unknown fixture mode")
	}
	tmp := modePath + ".new"
	if err := os.WriteFile(tmp, []byte(mode), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, modePath)
}

// Log request provenance without recording object names or signed headers.
func logFault(r *http.Request, fault string) {
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	log.Printf("fault=%s client=%s method=%s", fault, host, strings.ToUpper(r.Method))
}
