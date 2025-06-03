package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type KVServer struct {
	mu      sync.RWMutex
	config  *config
	httpSrv *http.Server
}

// NewKVServer creates a new KVServer cluster.
func NewKVServer(nservers int) *KVServer {
	cfg := make_config(nil, nservers, false, -1)
	return &KVServer{
		config: cfg,
	}
}

// Shutdown shuts down all servers in the cluster and the HTTP server.
func (kvs *KVServer) Shutdown() {
	for i := 0; i < kvs.config.n; i++ {
		kvs.config.ShutdownServer(i)
	}

	if kvs.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := kvs.httpSrv.Shutdown(ctx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}
}

// GetHandler handles HTTP GET requests.
func (kvs *KVServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	ck := kvs.config.MakeClient(kvs.config.All())
	value := ck.Get(key)

	response := map[string]string{"value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// PutHandler handles HTTP PUT requests.
func (kvs *KVServer) PutHandler(w http.ResponseWriter, r *http.Request) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "Key and value are required", http.StatusBadRequest)
		return
	}

	ck := kvs.config.MakeClient(kvs.config.All())
	ck.Put(key, value)

	w.WriteHeader(http.StatusOK)
}

// DeleteHandler handles HTTP DELETE requests.
func (kvs *KVServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	ck := kvs.config.MakeClient(kvs.config.All())
	ck.Put(key, "") // Simulate delete by setting an empty value

	w.WriteHeader(http.StatusOK)
}

// StartHTTPServer starts the HTTP server for the KVServer.
func (kvs *KVServer) StartHTTPServer(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", kvs.GetHandler)
	mux.HandleFunc("/put", kvs.PutHandler)
	mux.HandleFunc("/delete", kvs.DeleteHandler)

	kvs.httpSrv = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	log.Printf("Starting HTTP server on %s", kvs.httpSrv.Addr)
	go func() {
		if err := kvs.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

// Example main function to keep process alive
func main() {
	// Define all command-line flags first
	portPtr := flag.String("port", "8080", "HTTP server port")
	raftRepliPtr := flag.Int("raftrepli", 3, "Number of raft replicated servers")
	flag.Parse()

	// Get port from environment or command-line
	port := os.Getenv("PORT")
	if port == "" {
		port = *portPtr
	}

	// Get raftrepli from environment or command-line
	raftRepliStr := os.Getenv("RAFT_REPLI")
	if raftRepliStr == "" {
		raftRepliStr = strconv.Itoa(*raftRepliPtr)
	}
	shardCount, _ := strconv.Atoi(raftRepliStr)

	kvs := NewKVServer(shardCount)
	kvs.StartHTTPServer(port)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal: %v, shutting down...", sig)
	kvs.Shutdown()
}
