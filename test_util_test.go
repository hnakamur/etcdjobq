package etcdjobq_test

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type etcdCluster struct {
	etcds [3]*embed.Etcd
}

func startEtcdCluster(t *testing.T) *etcdCluster {
	ports, err := freeport.GetFreePorts(6)
	if err != nil {
		t.Fatalf("cannot get 6 free ports: %s", err)
	}

	dir := t.TempDir()
	var c etcdCluster
	initialCluster := c.initialCluster(ports)
	for i := 0; i < len(c.etcds); i++ {
		t.Logf("startEmbeddedEtcd i=%d, clientURL=%s, peerURL=%s, cluster=%s",
			i, c.clientURL(ports, i),
			c.peerURL(ports, i),
			initialCluster)
		c.etcds[i] = c.startEmbeddedEtcd(t,
			c.serverName(i),
			filepath.Join(dir, c.serverName(i)),
			c.clientURL(ports, i),
			c.peerURL(ports, i),
			initialCluster)
	}
	c.waitServersReady(t)
	return &c
}

func (c *etcdCluster) initialCluster(ports []int) string {
	var parts [3]string
	for i := 0; i < len(c.etcds); i++ {
		parts[i] = fmt.Sprintf("%s=%s", c.serverName(i), c.peerURL(ports, i))
	}
	return strings.Join(parts[:], ",")
}

func (c *etcdCluster) serverName(i int) string {
	return fmt.Sprintf("server%d", i+1)
}

func (c *etcdCluster) clientURL(ports []int, i int) string {
	return fmt.Sprintf("http://localhost:%d", ports[2*i])
}

func (c *etcdCluster) peerURL(ports []int, i int) string {
	return fmt.Sprintf("http://localhost:%d", ports[2*i+1])
}

func (c *etcdCluster) startEmbeddedEtcd(t *testing.T, name, dir string, clientURL, peerURL, initialCluster string) *embed.Etcd {
	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = dir
	lcurl, _ := url.Parse(clientURL)
	lpurl, _ := url.Parse(peerURL)
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}
	cfg.AdvertisePeerUrls = []url.URL{*lpurl}
	cfg.InitialCluster = initialCluster

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("failed to start embedded etcd: %v", err)
	}
	return e
}

func (c *etcdCluster) waitServersReady(t *testing.T) {
	for i := 0; i < len(c.etcds); i++ {
		e := c.etcds[i]
		select {
		case <-e.Server.ReadyNotify():
			t.Log("Embedded etcd server is ready!")
		case <-time.After(5 * time.Second):
			e.Server.Stop()
			t.Fatal("Embedded etcd server took too long to start")
		}
	}
}

func (c *etcdCluster) Close() {
	for i := 0; i < len(c.etcds); i++ {
		c.etcds[i].Close()
	}
}

func (c *etcdCluster) endpoints() []string {
	var ep []string
	for i := 0; i < len(c.etcds); i++ {
		for _, u := range c.etcds[i].Server.Cfg.ClientURLs {
			ep = append(ep, u.String())
		}
	}
	return ep
}

func (c *etcdCluster) newClient(t *testing.T) *clientv3.Client {
	endpoints := c.endpoints()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("cannot connect to etcd server at %s, err: %v", strings.Join(endpoints, ","), err)
	}
	return cli

}
