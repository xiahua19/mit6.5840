package kvraft

const (
	OK                 = "OK"
	ErrKeyNotExist     = "ErrKeyNotExist"
	ErrNotLeader       = "ErrNotLeader"
	ErrHandleOpTimeOut = "ErrHandleOpTimeOut"
	ErrChanClose       = "ErrChanClose"
	ErrLeaderOutDated  = "ErrLeaderOutDated"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	Seq        uint64
	Identifier int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key        string
	Seq        uint64
	Identifier int64
}

type GetReply struct {
	Err   Err
	Value string
}
