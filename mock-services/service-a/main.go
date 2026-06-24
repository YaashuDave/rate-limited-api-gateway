package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Simulate optional artificial latency via ?delay=ms query param.
		if d := r.URL.Query().Get("delay"); d != "" {
			var ms int
			if _, err := fmt.Sscanf(d, "%d", &ms); err == nil && ms > 0 {
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"service": "service-a",
			"path":    r.URL.Path,
			"host":    r.Host,
		})
	})

	log.Printf("service-a listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
