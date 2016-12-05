package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	servers map[string]time.Time
	cv      View // current view
	ack     bool // if current view acked
	nv      View // next view
	valid   bool // if next view existed
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.servers[args.Me] = time.Now()
	now := time.Now()

	// accept any server at all as the first primary
	if vs.cv.Viewnum == 0 && vs.cv.Primary == "" {
		vs.cv.Viewnum = 1
		vs.cv.Primary = args.Me
		vs.cv.Backup = ""
		goto ret
	}

	// if primary become empty again, report error
	if vs.cv.Primary == "" {
		log.Printf("Primary become empty again, system failed\n")
		os.Exit(1)
	}

	if vs.ack {
		// if there is no backup and there is an idle server
		if args.Me != vs.cv.Primary && vs.cv.Backup == "" {
			vs.cv.Viewnum = vs.cv.Viewnum + 1
			vs.cv.Backup = args.Me
			vs.ack = false
			goto ret
		}

		// if primary failed and restart
		if args.Me == vs.cv.Primary && args.Viewnum == 0 {
			vs.cv.Viewnum = vs.cv.Viewnum + 1
			op := vs.cv.Primary
			ob := vs.cv.Backup
			vs.cv.Primary = vs.cv.Backup
			vs.cv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == op || server == ob {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.cv.Backup = server
				}
			}
			vs.ack = false
			goto ret
		}

		// if backup failed and restart
		if args.Me == vs.cv.Backup && args.Viewnum == 0 {
			vs.cv.Viewnum = vs.cv.Viewnum + 1
			op := vs.cv.Primary
			ob := vs.cv.Backup
			vs.cv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == op || server == ob {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.cv.Backup = server
				}
			}
			vs.ack = false
			goto ret
		}
	} else {
		// if there is no backup and there is an idle server
		if args.Me != vs.cv.Primary && vs.cv.Backup == "" {
			vs.nv.Viewnum = vs.cv.Viewnum + 1
			vs.nv.Primary = vs.cv.Primary
			vs.nv.Backup = args.Me
			vs.valid = true
			goto ret
		}
		// if it is the ping from primary and current view not acked
		if args.Viewnum == vs.cv.Viewnum && args.Me == vs.cv.Primary {
			if vs.valid {
				// use next view to replace current view
				vs.cv = vs.nv
				vs.valid = false
			} else {
				vs.ack = true
			}
			goto ret
		}

		// if primary failed and restart
		if args.Me == vs.cv.Primary && args.Viewnum == 0 {
			vs.nv.Viewnum = vs.cv.Viewnum + 1
			vs.nv.Primary = vs.cv.Backup
			vs.nv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == vs.cv.Primary || server == vs.cv.Backup {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.nv.Backup = server
				}
			}
			vs.valid = true
			goto ret
		}

		/*		// if backup failed and restart
				if args.Me == vs.cv.Backup && args.Viewnum == 0 {
					vs.nv.Viewnum = vs.cv.Viewnum + 1
					vs.nv.Backup = ""
					vs.nv.Primary = vs.cv.Primary
					// choose an idle server as backup
					for server, t := range vs.servers {
						if server == vs.cv.Primary || server == vs.cv.Backup {
							continue
						}
						if now.Sub(t) < DeadPings*PingInterval {
							vs.nv.Backup = server
						}
					}
					vs.valid = true
					goto ret
				}*/
	}

ret:
	reply.View = vs.cv
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.cv

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// if no primary exists, return simply
	if vs.cv.Primary == "" {
		return
	}

	now := time.Now()

	if vs.ack {
		// primary timeout, if current view acked, directly to next view
		if now.Sub(vs.servers[vs.cv.Primary]) > DeadPings*PingInterval {
			op := vs.cv.Primary
			ob := vs.cv.Backup
			vs.cv.Viewnum++
			vs.cv.Primary = vs.cv.Backup
			vs.cv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == op || server == ob {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.cv.Backup = server
					break
				}
			}
			vs.ack = false
		}

		// backup timeout, if current view acked, directly to next view
		if now.Sub(vs.servers[vs.cv.Backup]) > DeadPings*PingInterval {
			op := vs.cv.Primary
			ob := vs.cv.Backup
			vs.cv.Viewnum++
			vs.cv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == op || server == ob {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.cv.Backup = server
					break
				}
			}
			vs.ack = false
		}
	} else {
		// primary timeout
		if now.Sub(vs.servers[vs.cv.Primary]) > DeadPings*PingInterval {
			vs.nv.Viewnum = vs.cv.Viewnum + 1
			vs.nv.Primary = vs.cv.Backup
			vs.nv.Backup = ""
			// choose an idle server as backup
			for server, t := range vs.servers {
				if server == vs.cv.Primary || server == vs.cv.Backup {
					continue
				}
				if now.Sub(t) < DeadPings*PingInterval {
					vs.nv.Backup = server
					break
				}
			}
			vs.valid = true
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.servers = make(map[string]time.Time)
	vs.ack = false
	vs.valid = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
