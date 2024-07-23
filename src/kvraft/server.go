package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HandleOpTimeOut = time.Millisecond * 500
)

const (
	OpGet int = iota
	OpPut
	OpAppend
)

type Op struct {
	OpType     int
	Key        string
	Value      string
	Seq        uint64
	Identifier int64
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32                // set by Kill()
	waiCh      map[int]*chan result // map startIndex to ch (RPC handler channel which wait commit info)
	historyMap map[int64]*result    // map identifier to *result

	maxraftstate int // snapshot if log grows this big
	maxMapLen    int
	db           map[string]string
}

type result struct {
	LastSeq uint64 // sequence number
	ResTerm int    // the term when apply is committed
	Err     Err
	Value   string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	opArgs := &Op{
		OpType:     OpGet,
		Seq:        args.Seq,
		Key:        args.Key,
		Identifier: args.Identifier,
	}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		return result{
			Err:   ErrNotLeader,
			Value: "",
		}
	}

	kv.mu.Lock()
	newCh := make(chan result)
	kv.waiCh[startIndex] = &newCh
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			return
		} else if !success {
			res.Err = ErrChanClose
			return
		} else {
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	opArgs := &Op{
		Seq:        args.Seq,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
	}

	if args.Op == "Put" {
		opArgs.OpType = OpPut
	} else {
		opArgs.OpType = OpAppend
	}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh

		if log.CommandValid {
			op := log.Command.(Op)
			kv.mu.Lock()

			var res result
			needApply := false

			if hisMap, exist := kv.historyMap[op.Identifier]; exist {
				if hisMap.LastSeq == op.Seq {
					res = *hisMap
				} else if hisMap.LastSeq < op.Seq {
					needApply = true
				}
			} else {
				needApply = true
			}

			_, isLeader := kv.rf.GetState()

			if needApply {
				res = kv.DBExecute(&op, isLeader)
				res.ResTerm = log.SnapshotTerm
				kv.historyMap[op.Identifier] = &res
			}

			if !isLeader {
				kv.mu.Unlock()
				continue
			}

			ch, exist := kv.waiCh[log.CommandIndex]
			if !exist {
				kv.mu.Unlock()
				continue
			}

			kv.mu.Unlock()

			func() {
				defer func() {
					if recover() != nil {

					}
				}()
				res.ResTerm = log.SnapshotTerm
				*ch <- res
			}()
		}
	}
}

func (kv *KVServer) DBExecute(op *Op, isLeader bool) (res result) {
	res.LastSeq = op.Seq

	switch op.OpType {
	case OpGet:
		val, exist := kv.db[op.Key]
		if exist {
			res.Value = val
			return
		} else {
			res.Err = ErrKeyNotExist
			res.Value = ""
			return
		}
	case OpPut:
		kv.db[op.Key] = op.Value
		return
	case OpAppend:
		val, exist := kv.db[op.Key]
		if exist {
			kv.db[op.Key] = val + op.Value
			return
		} else {
			kv.db[op.Key] = op.Value
			return
		}
	}
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.historyMap = make(map[int64]*result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int]*chan result)

	go kv.ApplyHandler()

	return kv
}
