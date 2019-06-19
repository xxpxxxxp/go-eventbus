package common

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type sealedZkConn struct {
	*zk.Conn
	connectedSemaphore *Semaphore
}

var (
	zkConn *sealedZkConn
	once   sync.Once
)

func InitZk(hosts string) (err error) {
	once.Do(func() {
		zkConn = &sealedZkConn{nil, NewSemaphore()}
		var zkEventChan <-chan zk.Event
		zkConn.Conn, zkEventChan, err = zk.Connect(strings.Split(hosts, ","), time.Second*30)
		go func() {
			for {
				var event zk.Event
				var ok bool
				if event, ok = <-zkEventChan; !ok {
					return
				}

				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateConnected, zk.StateHasSession:
						zkConn.connectedSemaphore.Signal()
					default:
						zkConn.connectedSemaphore.Reset()
					}
				}
			}
		}()
	})

	return
}

func (zc *sealedZkConn) bulkCreateZnodes(nodes []string, data []byte, flags int32, acl []zk.ACL) error {
	createRequests := make([]interface{}, len(nodes))
	for i, node := range nodes {
		createRequests[i] = &zk.CreateRequest{Path: node, Data: data, Acl: acl, Flags: flags}
	}
	if _, err := zc.Multi(createRequests...); err != nil {
		return err
	}
	return nil
}

func (zc *sealedZkConn) createZnodeRecursive(node string) (string, error) {
	if exist, _, err := zc.Exists(node); err != nil {
		return node, err
	} else if exist {
		return node, zk.ErrNodeExists
	}

	if father, err := zc.createZnodeRecursive(path.Dir(node)); err != nil && err != zk.ErrNodeExists {
		return father, err
	}

	return zc.Create(node, []byte(nil), int32(0), zk.WorldACL(zk.PermAll))
}

// unused methods but I think we'll eventually use them =================================
const retryTime = 2

func (zc *sealedZkConn) zkRetry(fn func() error, timeoutInSec int) error {
	var err error
	for i := 0; i < retryTime; i++ {
		zc.connectedSemaphore.WaitFor(time.Second * time.Duration(timeoutInSec))
		if err = fn(); err == nil {
			break
		}
	}
	return err
}

func (zc *sealedZkConn) bulkDeleteZnodes(nodes []string) error {
	deleteRequests := make([]interface{}, len(nodes))
	for i, node := range nodes {
		deleteRequests[i] = &zk.DeleteRequest{Path: node, Version: -1}
	}
	if _, err := zc.Multi(deleteRequests...); err != nil {
		return err
	}
	return nil
}

func (zc *sealedZkConn) removeZnode(node string) error {
	if exist, _, err := zc.Exists(node); err != nil {
		return err
	} else if !exist {
		return zk.ErrNoNode // path not exist
	}

	return zc.Delete(node, int32(-1))
}

// RemoveRecursiveZnode
func (zc *sealedZkConn) removeRecursiveZnode(root string) error {
	if err := zc.rmrZnode(root); err != nil {
		return err
	}

	// heal O.C.D of your guys
	return zc.Delete(root, int32(-1))
}

func (zc *sealedZkConn) rmrZnode(root string) error {
	if exist, _, err := zc.Exists(root); err != nil || !exist {
		return err
	}

	// rmr sub tree DFS
	var children []string
	var err error
	if children, _, err = zc.Children(root); err != nil {
		return err
	}

	if len(children) > 0 {
		childrenFullPath := make([]string, len(children))
		for i, child := range children {
			childrenFullPath[i] = fmt.Sprintf("%s/%s", root, child)
			if err := zc.rmrZnode(childrenFullPath[i]); err != nil {
				return err
			}
		}
		if err := zc.bulkDeleteZnodes(childrenFullPath); err != nil {
			return err
		}
	}
	return nil
}

func (zc *sealedZkConn) GetSubZnodes(node string) (subNodes []string, err error) {
	if exist, _, e := zc.Exists(node); e != nil || !exist {
		return nil, errors.New("node not exist")
	}
	subNodes, _, err = zc.Children(node)
	return
}
