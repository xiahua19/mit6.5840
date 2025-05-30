package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
)

type KVClient struct {
	serverURLs    []string
	currentServer int
}

func MakeKVClient(serverURLs []string) *KVClient {
	return &KVClient{
		serverURLs:    serverURLs,
		currentServer: 0,
	}
}

func (ck *KVClient) Get(key string) string {
	url := fmt.Sprintf("%s/get?key=%s", ck.serverURLs[ck.currentServer], key)
	resp, err := http.Get(url)
	if err != nil {
		ck.currentServer = (ck.currentServer + 1) % len(ck.serverURLs)
		return ""
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}
	return result["value"]
}

func (ck *KVClient) PutAppend(key string, value string, op string) {
	url := fmt.Sprintf("%s/%s?key=%s&value=%s", ck.serverURLs[ck.currentServer], op, key, value)
	resp, err := http.Get(url)
	if err != nil {
		ck.currentServer = (ck.currentServer + 1) % len(ck.serverURLs)
		return
	}
	defer resp.Body.Close()
}

func (ck *KVClient) Put(key string, value string) {
	ck.PutAppend(key, value, "set")
}

func (ck *KVClient) Delete(key string) {
	ck.PutAppend(key, "", "delete")
}

func main() {
	var serverURLs []string
	urlFlag := flag.String("url", "http://localhost:8081", "Server URL to connect to")
	flag.Parse()

	if envURLs := os.Getenv("KV_SERVER_URLS"); envURLs != "" {
		serverURLs = strings.Split(envURLs, ",")
	} else {
		serverURLs = []string{*urlFlag}
	}

	ck := MakeKVClient(serverURLs)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("kvclient> ")
		cmd, _ := reader.ReadString('\n')
		cmd = strings.TrimSpace(cmd)
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			value := ck.Get(parts[1])
			fmt.Printf("%s\n", value)
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			ck.Put(parts[1], parts[2])
			fmt.Println("OK")
		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			ck.Delete(parts[1])
			fmt.Println("OK")
		case "exit":
			os.Exit(0)
		default:
			fmt.Println("Unknown command. Available commands: get, set, delete, exit")
		}
	}
}
