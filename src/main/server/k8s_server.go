package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"6.5840/kvraft"
	"6.5840/labrpc"
	"6.5840/raft"
)

// KVServerModule encapsulates the functionality of a KVServer for containerized deployment.
type KVServerModule struct {
	server     *kvraft.KVServer
	httpServer *http.Server
	clientEnd  *labrpc.ClientEnd
}

// NewKVServerModule creates a new KVServerModule instance.
func NewKVServerModule(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, backendtype kvraft.BackendType, port string) *KVServerModule {
	module := &KVServerModule{
		server:    kvraft.StartKVServer(servers, me, persister, maxraftstate, backendtype),
		clientEnd: servers[me],
	}
	module.startHTTPServer(port)
	return module
}

// startHTTPServer initializes an HTTP server for REST API access.
func (m *KVServerModule) startHTTPServer(port string) {
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		args := &kvraft.GetArgs{Key: key}
		reply := &kvraft.GetReply{}
		ok := m.clientEnd.Call("KVServer.Get", args, reply)
		if !ok || reply.Err != kvraft.OK {
			json.NewEncoder(w).Encode(map[string]string{"error": "RPC call failed"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"value": reply.Value})
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		args := &kvraft.PutAppendArgs{Key: key, Value: value, Op: "Put"}
		reply := &kvraft.PutAppendReply{}
		ok := m.clientEnd.Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err != kvraft.OK {
			json.NewEncoder(w).Encode(map[string]string{"error": "RPC call failed"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		args := &kvraft.PutAppendArgs{Key: key, Value: "", Op: "Delete"}
		reply := &kvraft.PutAppendReply{}
		ok := m.clientEnd.Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err != kvraft.OK {
			json.NewEncoder(w).Encode(map[string]string{"error": "RPC call failed"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "OK"})
	})

	m.httpServer = &http.Server{Addr: ":" + port}
	go func() {
		if err := m.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()
}

// Run starts the KVServerModule and blocks until the server is killed.
func (m *KVServerModule) Run() {
	// Block until the server is killed.
	for !m.server.Killed() {
		time.Sleep(1 * time.Second)
	}
	m.httpServer.Close()
}

// GetConfigFromEnv reads configuration from environment variables.
func GetConfigFromEnv() (servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, backendtype kvraft.BackendType, err error) {
	// Parse server index (me).
	meStr := os.Getenv("KV_SERVER_INDEX")
	if meStr == "" {
		return nil, 0, nil, 0, 0, fmt.Errorf("KV_SERVER_INDEX environment variable is required")
	}
	// Extract numeric part (e.g., "kvserver-0" -> "0")
	meStr = strings.TrimPrefix(meStr, "kvserver-")
	me, err = strconv.Atoi(meStr)
	if err != nil {
		return nil, 0, nil, 0, 0, fmt.Errorf("invalid KV_SERVER_INDEX (must end with a number): %v", err)
	}

	// Parse maxraftstate.
	maxraftstateStr := os.Getenv("KV_MAXRAFTSTATE")
	maxraftstate = -1 // Default to no snapshotting.
	if maxraftstateStr != "" {
		maxraftstate, err = strconv.Atoi(maxraftstateStr)
		if err != nil {
			return nil, 0, nil, 0, 0, fmt.Errorf("invalid KV_MAXRAFTSTATE: %v", err)
		}
	}

	// Parse backend type.
	backendtypeStr := os.Getenv("KV_BACKEND_TYPE")
	backendtype = kvraft.Memory // Default to in-memory backend.
	if backendtypeStr != "" {
		backendtypeInt, err := strconv.Atoi(backendtypeStr)
		if err != nil {
			return nil, 0, nil, 0, 0, fmt.Errorf("invalid KV_BACKEND_TYPE: %v", err)
		}
		backendtype = kvraft.BackendType(backendtypeInt)
	}

	// Parse server addresses.
	serverAddrs := os.Getenv("KV_SERVER_ADDRESSES")
	if serverAddrs == "" {
		return nil, 0, nil, 0, 0, fmt.Errorf("KV_SERVER_ADDRESSES environment variable is required")
	}
	addrs := strings.Split(serverAddrs, ",")
	servers = make([]*labrpc.ClientEnd, len(addrs))

	// Assume the network instance is shared externally.
	net := labrpc.MakeNetwork()
	for i, addr := range addrs {
		end := net.MakeEnd(addr)
		servers[i] = end
	}

	// Initialize persister (simplified for demo).
	persister = raft.MakePersister()

	return servers, me, persister, maxraftstate, backendtype, nil
}

// Main function for containerized deployment.
func main() {
	port := flag.String("port", "8081", "HTTP server port")
	flag.Parse()

	servers, me, persister, maxraftstate, backendtype, err := GetConfigFromEnv()
	if err != nil {
		log.Fatalf("Failed to read configuration from environment: %v", err)
	}

	module := NewKVServerModule(servers, me, persister, maxraftstate, backendtype, *port)
	module.Run()
}
