
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "container/list"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  tmpview View
  view View
  backups *list.List
  tickcounter map[string]int
}

func (vs *ViewServer) CopyViewIfNeeded() bool{

  // judge if vs.tmpview and vs.view are same or not
  if vs.tmpview.Primary != vs.view.Primary || vs.tmpview.Backup != vs.view.Backup {
	vs.view.Viewnum++
    vs.view.Primary = vs.tmpview.Primary
    vs.view.Backup = vs.tmpview.Backup
    vs.view.Acked = false
    return true
  } 
  return false
}

func (vs *ViewServer) IsInBackupQueue(server string) bool{
	for e := vs.backups.Front(); e != nil; e = e.Next(){
	  if e.Value == server{
		return true
	  }
	}
    return false
}

func (vs *ViewServer) RemoveBackupElem(server string) bool{
	for e := vs.backups.Front(); e != nil; e = e.Next(){
	  if e.Value == server{
		vs.backups.Remove(e)
		return true
	  }
	}
    return false
}
//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  // no suspend there are at most two clients.
  // when view changed , first write new p/b into tmpview , when primary ackownledge the tmpview , view := tmpview
  vs.mu.Lock()
  if args.Viewnum == 0 {
    if vs.tmpview.Primary == ""{
      vs.tmpview.Primary = args.Me
      // check if tmpview and view are same value
	  vs.CopyViewIfNeeded()
    }else if vs.view.Primary == args.Me{
	  vs.tmpview.Primary = vs.view.Backup
	  vs.tmpview.Backup = args.Me
	  vs.CopyViewIfNeeded()
	}else if vs.view.Backup == ""{
      vs.tmpview.Backup = args.Me
      if vs.view.Acked {
       vs.CopyViewIfNeeded()
	  }
    }else if vs.view.Backup != args.Me{
      if !vs.IsInBackupQueue(args.Me){
	   fmt.Printf("push %s to queue\n", args.Me)
	   vs.backups.PushBack(args.Me)
	  }
	}
  } else {
    if vs.view.Primary == args.Me{
      // check if tmpview and view are same value
      //vs.CopyViewIfNeeded()
      if vs.view.Viewnum == args.Viewnum {
        vs.view.Acked = true
	  }
    }
  }
  //fmt.Println(args.Me)
  vs.tickcounter[args.Me] = 0
  reply.View = vs.view
  vs.mu.Unlock()
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = vs.view
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
  primary := vs.view.Primary
  backup := vs.view.Backup

  // check if p/b deaded
  for k, _ := range vs.tickcounter {
    if k != "" {
      vs.tickcounter[k]++
    }

    if vs.tickcounter[k] == DeadPings {
      if k == primary{
        // change backup to primary
        fmt.Printf("primary: %s is dead, now change backup(%s) to primary.\n", primary,  vs.tmpview.Backup)
        // should this place be tmpview to prevent "split brain"?
        vs.tmpview.Primary = vs.view.Backup
        //if backups list have elems , change one to view.Backup?
        elem := vs.backups.Front()
        if elem != nil{
         fmt.Printf("get backup(%s) form backups queue.\n", elem.Value.(string))
         vs.tmpview.Backup = elem.Value.(string)
         vs.RemoveBackupElem(vs.tmpview.Backup)
        } else{
         vs.tmpview.Backup = ""
        }
        if vs.view.Acked{
          vs.CopyViewIfNeeded()
        }
      } else if k == backup {
        fmt.Printf("backup: %s is dead, now change backup to blank.\n", backup)
        //if backups list have elems , change one to view.Backup
        elem := vs.backups.Front()
        if elem != nil{
         fmt.Printf("get backup(%s) form backups queue.\n", elem.Value.(string))
         vs.tmpview.Backup = elem.Value.(string)
         vs.RemoveBackupElem(vs.view.Backup)
        } else{
         vs.tmpview.Backup = ""
        }

        if vs.view.Acked{
          vs.CopyViewIfNeeded()
        }
      } else {
        fmt.Printf("backups list elem %s is dead, now delete it from queue.\n", backup)
        vs.RemoveBackupElem(k)
      }
      // todo when to change the view
      // vs.tickcounter[k] = 0
      delete(vs.tickcounter, k)
      //vs.view.Viewnum++
    }
  }
  vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.view = View{0, "", "", false}
  vs.tmpview = View{0, "", "", false}
  vs.backups = list.New() 
  vs.tickcounter = make(map[string]int)
  // Your vs.* initializations here.

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  //os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("tcp", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
