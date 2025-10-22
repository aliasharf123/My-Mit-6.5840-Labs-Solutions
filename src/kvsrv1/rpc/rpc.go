package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	ErrTimeout = "ErrTimeout"
)

type Tversion uint64

type PutArgs struct {
	Key      string
	Value    string
	Version  Tversion
	ClientId string // unique client id
	Seq      int64  // init (0)
}

type Reply struct {
	Err Err
}

type ReplyI interface {
	GetErr() Err
}

func (r *Reply) GetErr() Err {
	return r.Err
}

type PutReply struct {
	Reply
}

type GetArgs struct {
	Key      string
	ClientId string // unique client id
	Seq      int64  // init (0)
}

type GetReply struct {
	Value   string
	Version Tversion
	Reply
}
