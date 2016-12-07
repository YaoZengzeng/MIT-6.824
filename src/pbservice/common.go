package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	OpId      int64 // unique ID for every operation
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpId int64 // unique ID for every operation
}

type GetReply struct {
	Err   Err
	Value string
}

type InitBackupArgs struct {
	Db        map[string]string
	Duplicate map[int64]bool
}

type InitBackupReply struct {
	Err Err
}

type ForwardRequestArgs struct {
	Operation string
	OpId      int64
	Key       string
	Value     string
}

type ForwardRequestReply struct {
	Err Err
}

// Your RPC definitions here.
