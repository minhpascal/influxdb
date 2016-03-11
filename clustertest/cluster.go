// +build cluster
package clustertest

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/uuid"

	"github.com/influxdata/influxdb/models"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/client/v2"

	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"

	"github.com/influxdata/influxdb/cmd/influxd/run"
)

// Gap between ports on different nodes, e.g., 8188, 8288, 8388
const portJump = 100

// Cluster is the interface to a running Influxd cluster.
type Cluster interface {
	// Start initialises the cluster.
	Start() error

	// Stop releases any necessary resources.
	Stop() error

	// QueryAll runs the provided query on all data nodes in the cluster.
	QueryAll(cmd string, database string) <-chan response

	// QueryAny runs the provided query on an arbitrarily chosen node in the
	// cluster.
	QueryAny(cmd string, database string) response

	// Query the specified data node.
	Query(id int, cmd string, database string) response

	// WriteAny writes the provided points to an arbitrary node in the
	// cluster.
	WriteAny(config client.BatchPointsConfig, points ...string) response

	// Write the provided points to the specified node.
	Write(id int, config client.BatchPointsConfig, points ...string) response

	// NewDatabase generates a database name and creates it in the
	// cluster. It's possible to provide certain functional options to
	// which will be applied in order after the database is created.
	NewDatabase(options ...newDBOption) (string, error)

	// Info returns information about the configuration of the
	// cluster.
	Info() ClusterInformation
}

// ClusterInformation provides information about the cluster.
type ClusterInformation struct {
	// Number of data nodes in the cluster
	DataN int
	// TODO(edd) more fields to add I'm sure...
}

// A newDBOption is a functional option that provides some extra setup
// when creating a new Database.
type newDBOption func(c Cluster, name string) error

// withRP returns a functional option for NewDatabase that createa a new
// retention policy on the database. The retention policy will not be
// set as the default.
var withRP = func(name string, duration time.Duration, replication int) newDBOption {
	return func(c Cluster, dbName string) error {
		cmd := fmt.Sprintf("CREATE RETENTION POLICY %q ON %q DURATION %dm REPLICATION %d", name, dbName, int(duration.Minutes()), replication)
		if qr := c.QueryAny(cmd, ""); qr.err != nil {
			return qr.err
		}
		return nil
	}
}

// withDefaultRP returns a functional option for NewDatabase that sets
// the default retention policy after the database is created.
var withDefaultRP = func(duration time.Duration, replication int) newDBOption {
	return func(c Cluster, name string) error {
		cmd := fmt.Sprintf("ALTER RETENTION POLICY %q ON %q DURATION %dm REPLICATION %d", "default", name, int(duration.Minutes()), replication)
		if qr := c.QueryAny(cmd, ""); qr.err != nil {
			return qr.err
		}
		return nil
	}
}

// TODO(edd): implement a remote Cluster?
type remote struct{}

// local is a locally running Cluster.
type local struct {
	// Cluster base directory. All nodes will live in n[i] folders
	// under this path.
	baseDir string

	// Location of influxd binary.
	binPath string

	// Number of expected meta and data nodes in the cluster.
	metaN, dataN int

	// All configurations created for the cluster
	nodeConfs map[string]*run.Config

	// Entry point into the cluster. This node is queried to check all
	// other nodes are up and ready.
	entryAddr string

	// queryTimeout specifies how long to wait before timing out a query
	queryTimeout time.Duration

	mu      sync.RWMutex
	clients map[int]client.Client // clients for querying nodes

	// dataNodes maintains a mapping between a data/hybrid node's ID and
	// the address their HTTP service is bound to.
	dataNodes map[int]string

	// cmds keeps track of all the processes we started.
	cmds []influxProcess

	logger *log.Logger

	// running keeps track of if the cluster is running or not. It makes
	// implementing idempotent Start/Stop methods simpler.
	running bool
}

// influxProcess ties together the Cmd for controlling the influxd
// process and the io.Closer that will need to be closed when gracefully
// killing influxd.
type influxProcess struct {
	cmd *exec.Cmd
	fd  io.Closer
}

// newlocal creates configuration files and directories for a new
// local cluster.
//
// The location to the influxd binary must be provided. If the empty
// string is passed in then `influxd` will be used. When the caller is
// ready to start all nodes in the cluster call Start.
//
// NB (edd) temporary cluster directory will not be cleaned up at the
// moment, but currently it's using host OS's temp directory.
func newlocal(hybridN, metaN, dataN int, binPath string) (*local, error) {
	if metaN+hybridN < 1 {
		panic("cluster must have at least one meta node")
	} else if dataN+hybridN < 1 {
		panic("cluster must have at least one data node")
	}

	c := &local{
		binPath:      binPath,
		metaN:        hybridN + metaN,
		dataN:        hybridN + dataN,
		nodeConfs:    make(map[string]*run.Config),
		queryTimeout: 10 * time.Second,
		clients:      make(map[int]client.Client),
		logger:       log.New(os.Stdout, "", log.LstdFlags),
	}

	// Location of cluster files.
	var err error
	if c.baseDir, err = ioutil.TempDir("", "influx-integration"); err != nil {
		panic(err)
	}
	log.Println(c.baseDir)

	// Setup configuration files.
	joinArg := generateJoinArg(metaN+hybridN, mustptoi(meta.DefaultHTTPBindAddress))

	// TODO(edd): DRY these loops up.
	var firstNode int
	for i := 1; i <= hybridN; i++ {
		if firstNode == 0 {
			// specify first data node so we can calculate entry port
			firstNode = i
		}
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", i))
		conf := newConfig(nodePath, i, "hybrid")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	for i := 1; i <= metaN; i++ {
		id := i + hybridN
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", id))
		conf := newConfig(nodePath, id, "meta")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	for i := 1; i <= dataN; i++ {
		id := i + hybridN + metaN
		if firstNode == 0 {
			// specify first data node so we can calculate entry port
			firstNode = id
		}
		nodePath := path.Join(c.baseDir, fmt.Sprintf("n%d", id))
		conf := newConfig(nodePath, id, "data")
		conf.Join = joinArg
		c.nodeConfs[path.Join(nodePath, "config.toml")] = conf
	}

	c.entryAddr = "localhost" + mustShiftPort(httpd.DefaultBindAddress, firstNode*portJump)
	// Write out config files
	for pth, conf := range c.nodeConfs {
		// Generate the directories.
		if err := os.MkdirAll(path.Dir(pth), os.ModePerm); err != nil {
			return nil, err
		}

		fd, err := os.Create(pth)
		if err != nil {
			return nil, err
		}

		err = toml.NewEncoder(fd).Encode(conf)
		fd.Close()
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *local) Info() ClusterInformation {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ClusterInformation{
		DataN: c.dataN,
	}
}

// Start starts all of the Influxd processes in the cluster.
func (c *local) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	if err := c.startNodes(); err != nil {
		return err
	}
	if err := c.mapServers(20 * time.Second); err != nil {
		return err
	}

	// Open clients to each data node.
	for id, node := range c.dataNodes {
		conf := client.HTTPConfig{Timeout: c.queryTimeout, Addr: "http://" + node}
		clt, err := client.NewHTTPClient(conf)
		if err != nil {
			return err
		}
		c.clients[id] = clt
	}
	c.running = true
	return nil
}

// TODO(edd) Provide ability to do something with stdout and stderr
// of the process.
func (c *local) startNodes() error {
	if c.binPath == "" {
		c.binPath = "influxd"
	}
	for pth := range c.nodeConfs {
		cmd := exec.Command(c.binPath, fmt.Sprintf("-config=%s", pth))

		// Setup file to log process to.
		fd, err := os.Create(path.Join(path.Dir(pth), "influxd.log"))
		if err != nil {
			return err
		}
		cmd.Stdout, cmd.Stderr = fd, fd // exec ensure syncronised writes

		if err := cmd.Start(); err != nil {
			return err
		}
		c.cmds = append(c.cmds, influxProcess{cmd: cmd, fd: fd})
	}
	return nil
}

// mapServers generates a mapping between node ids and the ports they're
// HTTP services are listening on.
func (c *local) mapServers(timeout time.Duration) error {
	// We need to use the entry point to the cluster, as we don't know
	// about any other nodes yet.
	clt, err := client.NewHTTPClient(client.HTTPConfig{Addr: "http://" + c.entryAddr})
	if err != nil {
		return err
	}
	defer clt.Close()

	var (
		check   <-chan time.Time
		retries float64
		tc      = time.After(timeout)
	)

	check = time.After(0)
	for {
		select {
		case <-tc:
			return fmt.Errorf("timed out waiting for all servers")
		case <-check:
			resp, err := clt.Query(client.NewQuery("SHOW SERVERS", "", ""))
			if err != nil {
				d := time.Duration(math.Pow(2, retries)) * 50 * time.Millisecond
				check = time.After(d)
				c.logger.Printf("Retrying after %v: %v", d, err)
				retries++
				continue
			}

			if resp.Error() != nil {
				return resp.Error()
			}

			if len(resp.Results) == 0 {
				return fmt.Errorf("expected some results")
			}

			result, err := parseResult(ShowServers, resp.Results[0])
			if err != nil || len(result.metaServers) < c.metaN || len(result.dataServers) < c.dataN {
				d := time.Duration(math.Pow(2, retries)) * 50 * time.Millisecond
				check = time.After(d)
				c.logger.Printf("Retrying after %v: %v, %d, %d\n", d, err, len(result.metaServers), len(result.dataServers))
				retries++
				continue
			}
			c.dataNodes = result.dataServers
			return nil
		}
	}
}

// Stop terminates all influxd processes started by the suite.
func (c *local) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	// close all clients.
	c.logger.Print("Closing client connections")
	for id, clt := range c.clients {
		if err := clt.Close(); err != nil {
			// TODO(edd): should really go to stderr (or logger for errors)
			c.logger.Print(err)
		}
		delete(c.clients, id)
	}

	c.logger.Print("Killing influxd processes")
	for _, cmd := range c.cmds {
		c.logger.Printf("Killing pid: %d", cmd.cmd.Process.Pid)
		if err := cmd.cmd.Process.Signal(os.Interrupt); err != nil {
			// TODO(edd): should really go to stderr (or logger for errors)
			c.logger.Print(err)
		}

		if _, err := cmd.cmd.Process.Wait(); err != nil {
			c.logger.Print(err)
		}

		// Close the log file
		cmd.fd.Close()
	}

	c.running = false
	return nil
}

type response struct {
	nodeID int
	result client.Result
	err    error
}

// QueryAll runs the query on all data nodes in the cluster. QueryAll
// immediately returns a channel to the caller, and ensures that the
// channel is closed when all nodes have returned results.
func (c *local) QueryAll(cmd string, database string) <-chan response {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var (
		ch = make(chan response, len(c.dataNodes))
		wg sync.WaitGroup
	)

	for id := range c.dataNodes {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch <- c.Query(id, cmd, database)
		}(id)
	}

	// Close the channel when all results have been gathered
	go func() { wg.Wait(); close(ch) }()
	return ch
}

// QueryAny runs Query on an arbitrary data node, returning the response
// back to the caller.
func (c *local) QueryAny(cmd string, database string) response {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var chosen int
	for id := range c.dataNodes {
		chosen = id
		break
	}
	return c.query(chosen, cmd, database)
}

// Query runs the query on the specified data node.
func (c *local) Query(id int, cmd string, database string) response {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.query(id, cmd, database)
}

// query runs a query on a specified data node.
// Callers must manage a read lock on the local cluster appropriately.
func (c *local) query(id int, cmd string, database string) response {
	resp := response{nodeID: id}

	// Get client for node.
	clt, ok := c.clients[id]
	if !ok {
		resp.err = fmt.Errorf("cannot find client for node %d. Possibly cluster stoppage", id)
		return resp
	}

	// TODO(edd): handle precision?
	qr, err := clt.Query(client.NewQuery(cmd, database, ""))
	if err != nil {
		resp.err = err
		return resp
	}

	if qr.Error() != nil {
		resp.err = qr.Error()
		return resp
	}

	if len(qr.Results) == 0 {
		resp.err = fmt.Errorf("expected some results")
		return resp
	}

	resp.result = qr.Results[0]
	return resp
}

func (c *local) WriteAny(config client.BatchPointsConfig, points ...string) response {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var chosen int
	for id := range c.dataNodes {
		chosen = id
		break
	}
	return c.Write(chosen, config, points...)
}

// Write writes the provided points on the specified node.
func (c *local) Write(id int, config client.BatchPointsConfig, points ...string) response {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.write(id, config, points...)
}

// write writes the provided points on the specified node.
// Callers must manage a read lock on the local cluster appropriately.
func (c *local) write(id int, config client.BatchPointsConfig, points ...string) response {
	resp := response{nodeID: id}

	// Get client for node.
	clt, ok := c.clients[id]
	if !ok {
		resp.err = fmt.Errorf("cannot find client for node %d", id)
		return resp
	}

	// TODO(edd): handle precision?
	pts, err := models.ParsePointsString(strings.Join(points, "\n"))
	if err != nil {
		resp.err = err
		return resp
	}

	// Set some defaults on the config.
	if config.WriteConsistency == "" {
		config.WriteConsistency = "all"
	}

	bp, err := client.NewBatchPoints(config)
	if err != nil {
		resp.err = err
		return resp
	}

	for _, point := range pts {
		bp.AddPoint(&client.Point{Point: point})
	}

	if err = clt.Write(bp); err != nil {
		resp.err = err
		return resp
	}
	return resp
}

// NewDatabase generates a new database name of the form `db_x` where x
// is a UUID.
func (c *local) NewDatabase(options ...newDBOption) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := fmt.Sprintf("db_%s", uuid.TimeUUID().String())
	qr := c.QueryAny(fmt.Sprintf("CREATE DATABASE %q", name), "")
	if qr.err != nil {
		return "", qr.err
	}

	// Apply any functional options
	for _, option := range options {
		if err := option(c, name); err != nil {
			qr.err = err
			return "", nil
		}
	}
	return name, nil
}

// NewDatabaseWithDefaultRP generates a new database name of the form `db_x`
// where x is a UUID, and creates it within the cluster. It then ensures
// that the default retention policy for the database has the provided
// duration and replication factor.
func (c *local) NewDatabaseWithDefaultRP(duration time.Duration, replication int) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	name := fmt.Sprintf("db_%s", uuid.TimeUUID().String())
	qr := c.QueryAny(fmt.Sprintf("CREATE DATABASE %q", name), "")
	if qr.err != nil {
		return "", qr.err
	}

	alterRPCMD := fmt.Sprintf("ALTER RETENTION POLICY %q ON %q DURATION %dm REPLICATION %d", "default", name, int(duration.Minutes()), replication)
	if qr = c.QueryAny(alterRPCMD, ""); qr.err != nil {
		return "", qr.err
	}
	return name, nil
}

// NodesHavingPath counts how many data nodes in the cluster have the
// specified path in their data directory.
// The provided path should be relative to the node's data directory.
func (c *local) NodesHavingPath(pth string) (int, error) {
	var count int
	for id := range c.dataNodes {
		nPath := path.Join(c.baseDir, fmt.Sprintf("n%d/.influxdb", id), "data", pth)
		_, err := os.Stat(nPath)
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		} else if err == nil {
			count++
		}
	}
	return count, nil
}

// generateJoinArg generate a join string for joining to all the meta
// nodes in the cluster.
func generateJoinArg(n int, seedPort int) string {
	var join string
	for i := portJump; i <= n*portJump; i += portJump {
		join += fmt.Sprintf("localhost:%d", i+seedPort)
		if i+portJump <= n*portJump {
			join += ","
		}
	}
	return join
}