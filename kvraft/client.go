package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// Nima
	leaderId  int32 // to save some time while looking for the leader
	requestId int64 // for queries to be unique
	clientId  int64 // combination of clientId and requestId show the uniqueness of a query.
	//
}

func nrand() int64 { // to generate unique client IDs
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// Nima
	ck.leaderId = 0
	ck.requestId = 0
	ck.clientId = nrand()
	//
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// Nima
	leaderId := atomic.LoadInt32(&ck.leaderId)
	requestId := atomic.AddInt64(&ck.requestId, 1) // requestId += 1
	args := GetArgs{
		Key:       key,
		RequestId: requestId,
		ClientId:  ck.clientId,
	}
	server := leaderId
	value := ""
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply) // communication to the leader via RPC
		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				value = reply.Value
			}
			break
		}
	}
	atomic.StoreInt32(&ck.leaderId, server) // updating the leaderId for next attempts.
	return value
	//
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// Nima
	leaderId := atomic.LoadInt32(&ck.leaderId)
	requestId := atomic.AddInt64(&ck.requestId, 1) // requestId += 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}
	server := leaderId
	for ; ; server = (server + 1) % int32(len(ck.servers)) {
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			break // nothing required; because the value is filled automatically in server side.
		}
	}
	atomic.StoreInt32(&ck.leaderId, server)
	//
}

// the following functions call the PutAppend function with the right arguments.
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
