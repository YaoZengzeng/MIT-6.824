package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type PrepareArgs struct {
	Seq    int
	Number int

	// the follow used for Min()
	Index int
	Done  int
}

type PrepareReply struct {
	Ok         bool
	HighestNum int
	Status     Fate
	Value      interface{}
}

type AcceptArgs struct {
	Seq    int
	Number int
	Value  interface{}

	// the follow used for Min()
	Index int
	Done  int
}

type AcceptReply struct {
	Ok bool
}

type LearnArgs struct {
	Seq int

	// the follow used for Min()
	Index int
	Done  int
}

type LearnReply struct {
	Status Fate
	Value  interface{}
}

type DecidedArgs struct {
	Seq   int
	Value interface{}

	// the follow used for Min()
	Index int
	Done  int
}

type DecidedReply struct {
}

type acceptState struct {
	status Fate
	n_p    int // highest prepare seen
	n_a    int // highest accept seen
	v_a    interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances map[int]*acceptState
	done      map[int]int
	max       int // highest instance sequence known to this peer
	min       int // the max instance that could be forgotten
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.proposer(seq, v)
}

func (px *Paxos) proposer(seq int, v interface{}) {
	n := 1

	// if the instance has been forgotten, just return
	if seq <= px.min {
		return
	}

	for !px.learn(seq) {
		count := 0
		nt := n
		rn := 0 // rn is the highest number in the peer whcih value is not nil
		for i := 0; i < len(px.peers); i++ {
			reply, _ := px.prepare(i, nt, seq)
			if reply.Ok {
				count++
				if reply.Value != nil && reply.HighestNum > rn {
					v = reply.Value
					rn = reply.HighestNum
				}
			} else {
				if reply.HighestNum >= n {
					n = reply.HighestNum + 1
					if reply.Value != nil && reply.HighestNum > rn {
						v = reply.Value
						rn = reply.HighestNum
					}
				}
			}
			// if there is peer's status is Decided, then just broadcast it
			if reply.Status == Decided {
				// not enable jumping into if block
				for i := 0; i < len(px.peers); i++ {
					px.decided(i, seq, reply.Value)
				}
				continue
			}
		}

		// wait for majority in agreement
		if count < (len(px.peers)/2 + 1) {
			continue
		}
		count = 0
		for i := 0; i < len(px.peers); i++ {
			reply, _ := px.accept(i, nt, seq, v)
			if reply.Ok {
				count++
			}
		}

		// wait for majority in agreement
		if count < (len(px.peers)/2 + 1) {
			continue
		} else {
			// set all peers' status to be Decided
			for i := 0; i < len(px.peers); i++ {
				px.decided(i, seq, v)
			}
		}
	}

	return
}

func (px *Paxos) decided(i int, seq int, v interface{}) {
	args := &DecidedArgs{}
	args.Seq = seq
	args.Value = v
	args.Index = px.me
	args.Done = px.done[px.me]
	var reply DecidedReply

	if i == px.me {
		px.DecidedHandler(args, &reply)
		return
	}

	call(px.peers[i], "Paxos.DecidedHandler", args, &reply)

	return
}

func (px *Paxos) DecidedHandler(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if _, ok := px.instances[args.Seq]; ok == false {
		px.instances[args.Seq] = &acceptState{}
		px.instances[args.Seq].status = Pending
	}

	// never decide twice
	if px.instances[args.Seq].status == Decided {
		fmt.Printf("never decide twice, paxos %d, seq %d\n", px.me, args.Seq)
		return nil
	}

	px.instances[args.Seq].v_a = args.Value
	px.instances[args.Seq].status = Decided

	return nil
}

func (px *Paxos) learn(seq int) bool {
	args := &LearnArgs{}
	args.Seq = seq
	args.Index = px.me
	args.Done = px.done[px.me]
	var reply LearnReply

	for i := 0; i < len(px.peers); i++ {
		if i == px.me {
			px.LearnHandler(args, &reply)
		} else {
			ok := call(px.peers[i], "Paxos.LearnHandler", args, &reply)
			// pass the unavailable peers
			if ok == false {
				continue
			}
		}
		// if there is still a peer's status has not been decided, return false
		if reply.Status != Decided {
			return false
		}
	}

	return true
}

func (px *Paxos) LearnHandler(args *LearnArgs, reply *LearnReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Done > px.done[args.Index] {
		px.done[args.Index] = args.Done
	}

	if _, ok := px.instances[args.Seq]; ok == false {
		reply.Status = Pending
		return nil
	}

	reply.Status = px.instances[args.Seq].status
	reply.Value = px.instances[args.Seq].v_a

	return nil
}

func (px *Paxos) accept(i int, number int, seq int, v interface{}) (AcceptReply, error) {
	// prepare the arguments
	args := &AcceptArgs{}
	args.Seq = seq
	args.Number = number
	args.Value = v

	args.Index = px.me
	args.Done = px.done[px.me]
	var reply AcceptReply

	// in order to pass tests assuming unreliable network,
	// paxos should call the local acceptor through a function rather than RPC
	if i == px.me {
		err := px.AcceptHandler(args, &reply)
		return reply, err
	}

	// send an RPC request, wait for the reply
	ok := call(px.peers[i], "Paxos.AcceptHandler", args, &reply)
	if ok == false {
		return AcceptReply{}, fmt.Errorf("accept i : %d, number : %d, seq : %d failed", i, number, seq)
	}

	return reply, nil
}

func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Done > px.done[args.Index] {
		px.done[args.Index] = args.Done
	}

	// if the acceptor have not get any prepare request
	// just accept it, because the proposer must have get the majority
	if _, ok := px.instances[args.Seq]; ok != true {
		px.instances[args.Seq] = &acceptState{}
		px.instances[args.Seq].status = Pending
	}

	if args.Number < px.instances[args.Seq].n_p || px.instances[args.Seq].status == Decided {
		reply.Ok = false
	} else {
		px.instances[args.Seq].n_p = args.Number
		px.instances[args.Seq].n_a = args.Number
		px.instances[args.Seq].v_a = args.Value
		reply.Ok = true
	}

	return nil
}

func (px *Paxos) prepare(i int, number int, seq int) (PrepareReply, error) {
	// prepare the arguments
	args := &PrepareArgs{}
	args.Seq = seq
	args.Number = number

	args.Index = px.me
	args.Done = px.done[px.me]
	var reply PrepareReply

	// in order to pass tests assuming unreliable network,
	// paxos should call the local acceptor through a function rather than RPC
	if i == px.me {
		err := px.PrepareHandler(args, &reply)
		return reply, err
	}

	// send an RPC request, wait for the reply
	ok := call(px.peers[i], "Paxos.PrepareHandler", args, &reply)
	if ok == false {
		return PrepareReply{}, fmt.Errorf("prepare i : %d, number : %d, seq : %d failed", i, number, seq)
	}

	return reply, nil
}

func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if args.Done > px.done[args.Index] {
		px.done[args.Index] = args.Done
	}

	if _, ok := px.instances[args.Seq]; ok != true {
		i := &acceptState{
			status: Pending,
			n_p:    args.Number,
		}
		px.instances[args.Seq] = i
		reply.Ok = true
		reply.Status = px.instances[args.Seq].status
		if args.Seq > px.max {
			px.max = args.Seq
		}
		return nil
	}

	if args.Number > px.instances[args.Seq].n_p {
		px.instances[args.Seq].n_p = args.Number
		reply.Ok = true
		reply.HighestNum = px.instances[args.Seq].n_p
		reply.Status = px.instances[args.Seq].status
		reply.Value = px.instances[args.Seq].v_a
		return nil
	} else {
		reply.Ok = false
		reply.HighestNum = px.instances[args.Seq].n_p
		reply.Value = px.instances[args.Seq].v_a
		reply.Status = px.instances[args.Seq].status
		return nil
	}

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	px.done[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	min := px.done[0]
	for i := 1; i < len(px.done); i++ {
		if px.done[i] < min {
			min = px.done[i]
		}
	}

	if min > px.min {
		var oldMin int
		if px.min == -1 {
			oldMin = 0
		} else {
			oldMin = px.min
		}
		px.min = min

		// free the the information of old instances
		for i := oldMin; i <= px.min; i++ {
			delete(px.instances, i)
		}
	}

	return px.min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	// the instance has been forgotten
	if seq <= px.min {
		return Forgotten, nil
	}

	if _, ok := px.instances[seq]; ok {
		return px.instances[seq].status, px.instances[seq].v_a
	}

	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*acceptState)
	px.done = make(map[int]int)
	px.max = -1
	px.min = -1

	for i := 0; i < len(px.peers); i++ {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
