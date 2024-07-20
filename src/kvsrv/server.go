package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu            sync.Mutex
	Entries       map[string]string
	lastRequestId map[int64]int64  // key: clientId, value: lastRequestId
	appendReplies map[int64]string // key: clientId, value: last reply for Append
}

func (kv *KVServer) requestAlreadyDone(clientId int64, requestId int64) bool {
	lastRequestId, exists := kv.lastRequestId[clientId]
	if !exists {
		return false
	}
	return requestId <= lastRequestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, exists := kv.Entries[args.Key]; exists {
		reply.Value = val
		return
	}
	reply.Value = ""
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.requestAlreadyDone(args.ClientId, args.RequestId) {
		reply.Value = kv.Entries[args.Key]
		return
	}

	DPrintf("Put %v %v", args.Key, args.Value)
	kv.Entries[args.Key] = args.Value
	reply.Value = args.Value
	kv.lastRequestId[args.ClientId] = args.RequestId
	DPrintf("entries: %v", kv.Entries)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.requestAlreadyDone(args.ClientId, args.RequestId) {
		reply.Value = kv.appendReplies[args.ClientId]
		return
	}

	oldValue, exists := kv.Entries[args.Key]
	if exists {
		kv.Entries[args.Key] = oldValue + args.Value
		reply.Value = oldValue
	} else {
		kv.Entries[args.Key] = args.Value
		reply.Value = ""
	}

	kv.lastRequestId[args.ClientId] = args.RequestId
	kv.appendReplies[args.ClientId] = reply.Value
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		Entries:       make(map[string]string),
		lastRequestId: make(map[int64]int64),
		appendReplies: make(map[int64]string),
	}
	return kv
}
