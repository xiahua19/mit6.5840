package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

// const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func GetRandomElectTimeOut(rd *rand.Rand) int {
	plusMs := int(rd.Float64() * 150)

	return plusMs + ElectTimeOutBase
}

func (rf *Raft) ResetVoteTimer() {
	rdTimeOut := GetRandomElectTimeOut(rf.rd)
	rf.voteTimer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
	rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
}
