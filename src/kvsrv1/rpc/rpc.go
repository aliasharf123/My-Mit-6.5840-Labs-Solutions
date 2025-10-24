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

type ClientMeta struct {
	ClientId string // unique client id
	Seq      int64  // init (0)
}

type ClientMetaAccessor interface {
	GetClientMeta() (string, int64) // returns (ClientId, Seq)
}

func (c *ClientMeta) GetClientMeta() (string, int64) {
	return c.ClientId, c.Seq
}

type Args struct {
	Key string
}
type IArgs interface {
	GetKey() string
}

func (a *Args) GetKey() string {
	return a.Key
}

type PutArgs struct {
	Args
	Value   string
	Version Tversion
	ClientMeta
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
	Args
	ClientMeta
}

type GetReply struct {
	Value   string
	Version Tversion
	Reply
}
