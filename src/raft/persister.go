package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type Persister struct {
	mu            sync.Mutex
	raftstate     []byte
	snapshot      []byte
	raftStatePath string
	snapshotPath  string
}

// MakeDefaultPersister creates a Persister with default file paths.
func MakePersister() *Persister {
	return MakePersisterWithPath(filepath.Join("./file", fmt.Sprintf("raftstate-%s.bin", uuid.New().String())),
		filepath.Join("./file", fmt.Sprintf("snapshot-%s.bin", uuid.New().String())))
}

// MakePersister creates a Persister with specified file paths and loads data if files exist.
func MakePersisterWithPath(raftStatePath, snapshotPath string) *Persister {
	p := &Persister{
		raftStatePath: raftStatePath,
		snapshotPath:  snapshotPath,
	}
	// Attempt to load data from files if they exist
	if _, err := os.Stat(raftStatePath); err == nil {
		if err := p.LoadFromFile(); err != nil {
			DPrintf("failed to load data from file")
		}
	}
	return p
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersisterWithPath(ps.raftStatePath, ps.snapshotPath)
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
	// async write to files
	go os.WriteFile(ps.raftStatePath, ps.raftstate, 0644)
	go os.WriteFile(ps.snapshotPath, ps.snapshot, 0644)
	return nil
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// LoadFromFile loads raftstate and snapshot from files.
func (ps *Persister) LoadFromFile() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	raftstate, err := os.ReadFile(ps.raftStatePath)
	if err != nil {
		return err
	}
	snapshot, err := os.ReadFile(ps.snapshotPath)
	if err != nil {
		return err
	}
	ps.raftstate = raftstate
	ps.snapshot = snapshot
	return nil
}
