package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view      viewservice.View
	db        map[string]string
	duplicate map[int64]bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// The server isn't the active primary
	if pb.me != pb.view.Primary {
		reply.Err = "client request nonprimary"
		return nil
	}

	reply.Value = pb.db[args.Key]

	// forward request to backup
	return pb.ForwardRequestPrimary("Get", args.Key, "", args.OpId)
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// The server isn't the active primary
	if pb.me != pb.view.Primary {
		reply.Err = "client request nonprimary"
		return nil
	}

	_, ok := pb.duplicate[args.OpId]
	if ok == true {
		return nil
	} else {
		pb.duplicate[args.OpId] = true
	}

	if args.Operation == "Put" {
		pb.db[args.Key] = args.Value
	} else if args.Operation == "Append" {
		val := pb.db[args.Key]
		pb.db[args.Key] = val + args.Value
	} else {
		return errors.New("PBServer PutAppend Operation should only be Put or Append")
	}

	// forward request to backup
	return pb.ForwardRequestPrimary(args.Operation, args.Key, args.Value, args.OpId)
}

func (pb *PBServer) BackupInitialize(args *InitBackupArgs, reply *InitBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	/*	if pb.me != pb.view.Backup {
		reply.Err = "BackupInitialize: primary view is out of date, no longer backup"
		return nil
	}*/

	pb.db = args.Db
	pb.duplicate = args.Duplicate

	return nil
}

func (pb *PBServer) InitBackup() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	args := &InitBackupArgs{
		Db:        pb.db,
		Duplicate: pb.duplicate,
	}
retry:
	var reply InitBackupReply

	ok := call(pb.view.Backup, "PBServer.BackupInitialize", args, &reply)
	if ok == false {
		goto retry
	}
}

func (pb *PBServer) ForwardRequestBackup(args *ForwardRequestArgs, reply *ForwardRequestReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me == pb.view.Primary {
		reply.Err = "ForwardRequestBackup: primary view is out of date, no longer backup"
		return nil
	}

	_, ok := pb.duplicate[args.OpId]
	if ok == true {
		return nil
	} else {
		pb.duplicate[args.OpId] = true
	}

	if args.Operation == "Put" {
		pb.db[args.Key] = args.Value
	}
	if args.Operation == "Append" {
		val := pb.db[args.Key]
		pb.db[args.Key] = val + args.Value
	}

	return nil
}

func (pb *PBServer) ForwardRequestPrimary(operation string, key string, value string, OpId int64) error {
	args := &ForwardRequestArgs{
		Operation: operation,
		OpId:      OpId,
		Key:       key,
		Value:     value,
	}
retry:
	var reply ForwardRequestReply
	// if no backup, just skip
	if pb.view.Backup == "" {
		return nil
	}

	ok := call(pb.view.Backup, "PBServer.ForwardRequestBackup", args, &reply)
	if ok == false {
		goto retry
	}

	if reply.Err != "" {
		return errors.New(string(reply.Err))
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	view, _ := pb.vs.Ping(pb.view.Viewnum)
	oldview := pb.view
	//pb.mu.Lock()
	pb.view = view
	//pb.mu.Unlock()

	// if view changed, do some operations
	if oldview.Viewnum != view.Viewnum {
		// if current server is primary and backup changed, initialize it
		if pb.me == view.Primary && view.Backup != oldview.Backup && view.Backup != "" {
			go pb.InitBackup()
			//pb.InitBackup()
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.db = make(map[string]string)
	pb.duplicate = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
