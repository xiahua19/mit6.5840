package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// ShardLayerHTTP encapsulates the functionality of a sharding layer for routing HTTP requests to different pods.
type ShardLayerHTTP struct {
	shardURLs  []string // HTTP URLs for all shards
	shardCount int      // Total number of shards
}

// NewShardLayerHTTP creates a new ShardLayerHTTP instance.
func NewShardLayerHTTP(shardCount int, shardURLs []string) *ShardLayerHTTP {
	return &ShardLayerHTTP{
		shardURLs:  shardURLs,
		shardCount: shardCount,
	}
}

// hashKey computes a hash value for the given key to determine its shard.
func (s *ShardLayerHTTP) hashKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % s.shardCount
}

// getShardURL returns the HTTP URL for the shard responsible for the given key.
func (s *ShardLayerHTTP) getShardURL(key string) string {
	shard := s.hashKey(key)
	return s.shardURLs[shard]
}

// proxyRequest forwards an HTTP request to the target shard and returns the response.
func (s *ShardLayerHTTP) proxyRequest(w http.ResponseWriter, r *http.Request, endpoint string) {
	key := r.URL.Query().Get("key")
	shardURL := s.getShardURL(key)
	forwardURL := fmt.Sprintf("%s/%s?%s", shardURL, endpoint, r.URL.RawQuery)

	// Create a new request to forward to the target shard.
	forwardReq, err := http.NewRequest(r.Method, forwardURL, r.Body)
	if err != nil {
		info := fmt.Sprintf("Failed to create forward request %s", forwardURL)
		http.Error(w, info, http.StatusInternalServerError)
		return
	}
	forwardReq.Header = r.Header

	// Send the request to the target shard.
	client := &http.Client{}
	resp, err := client.Do(forwardReq)
	if err != nil {
		info := fmt.Sprintf("Failed to forward request %s", forwardURL)
		http.Error(w, info, http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Copy the response from the target shard to the original response writer.
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// startHTTPServer initializes an HTTP server for REST API access with sharding support.
func (s *ShardLayerHTTP) startHTTPServer(port string) {
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		s.proxyRequest(w, r, "get")
	})

	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		s.proxyRequest(w, r, "put")
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		s.proxyRequest(w, r, "delete")
	})

	server := &http.Server{Addr: ":" + port}
	log.Printf("Shard layer HTTP server started on port %s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// GetShardConfigFromEnv reads shard configuration from environment variables or command-line arguments.
func GetShardConfigFromEnv() (shardCount int, shardURLs []string, err error) {
	// Try to get from environment variables first
	shardCountStr := os.Getenv("SHARD_COUNT")
	shardURLsEnv := os.Getenv("SHARD_URLS")

	// If environment variables are not set, try command-line arguments
	if shardCountStr == "" || shardURLsEnv == "" {
		shardCountPtr := flag.Int("shard-count", 3, "Number of shard servers")
		shardURLsPtr := flag.String("shard-urls", "", "Comma-separated list of shard URLs")
		flag.Parse()

		if shardCountStr == "" {
			shardCountStr = strconv.Itoa(*shardCountPtr)
		}
		if shardURLsEnv == "" {
			shardURLsEnv = *shardURLsPtr
		}
	}

	// Validate and parse
	if shardCountStr == "" {
		return 0, nil, fmt.Errorf("SHARD_COUNT is required (either via environment or command-line)")
	}
	shardCount, err = strconv.Atoi(shardCountStr)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid SHARD_COUNT: %v", err)
	}

	if shardURLsEnv == "" {
		return 0, nil, fmt.Errorf("SHARD_URLS is required (either via environment or command-line)")
	}
	shardURLs = strings.Split(shardURLsEnv, ",")

	return shardCount, shardURLs, nil
}

// Main function for the HTTP-based shard layer.
func main() {
	port := os.Getenv("SHARD_LAYER_PORT")
	if port == "" {
		portStr := flag.String("port", "6666", "HTTP server port")
		port = *portStr
	}

	shardCount, shardURLs, err := GetShardConfigFromEnv()
	if err != nil {
		log.Fatalf("Failed to read shard configuration: %v", err)
	}

	shardLayer := NewShardLayerHTTP(shardCount, shardURLs)
	shardLayer.startHTTPServer(port)
}
