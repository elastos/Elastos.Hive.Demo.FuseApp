// Package hive implements an IPFS Cluster Client component. It
// uses the HIVE HTTP API to communicate to Hive Cluster.

package hive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/logger"
)

// DNSTimeout is used when resolving DNS multiaddresses in this module
var DNSTimeout = 5 * time.Second
var HiveRequestTimeout = 60 * 5 * time.Second

// updateMetricsMod only makes updates to informer metrics
// on the nth occasion. So, for example, for every BlockPut,
// only the 10th will trigger a SendInformerMetrics call.
var updateMetricMod = 10

// Connector implements the IPFSConnector interface
// and provides a component which  is used to perform
// on-demand requests against the configured IPFS daemom
// (such as a pin request).
type Connector struct {
	ctx    context.Context
	cancel func()

	nodeAddr string

	client *http.Client // client to ipfs daemon

	updateMetricMutex sync.Mutex
	updateMetricCount int

	shutdownLock sync.Mutex
	shutdown     bool
	wg           sync.WaitGroup
}

type ipfsError struct {
	Message string
}

type ipfsPinType struct {
	Type string
}

type ipfsPinLsResp struct {
	Keys map[string]ipfsPinType
}

type ipfsIDResp struct {
	ID        string
	Addresses []string
}

type ipfsSwarmPeersResp struct {
	Peers []ipfsPeer
}

type ipfsPeer struct {
	Peer string
}

type ipfsStream struct {
	Protocol string
}

type ipfsKeyGenResp struct {
	Name string
	Id   string
}

type ipfsKeyRenameResp struct {
	Was       string
	Now       string
	Id        string
	Overwrite bool
}

type ipfsKeyListResp struct {
	Keys []ipfsKey
}
type ipfsKey struct {
	Name string
	Id   string
}

// IPFSRepoStat wraps information about the IPFS repository.
type respIPFSRepoStat struct {
	RepoSize   uint64
	StorageMax uint64
}

// UIDKey wraps secret private key.
type respUIDKey struct {
	UID  string
	Key  []byte
	Root string
}

// UIDSecret wraps node register keys in the Hive Cluster.
type respUIDSecret struct {
	UID    string
	PeerID string
}

// UIDRenew wraps node keys renaming in the Hive Cluster.
type respUIDRenew struct {
	UID    string
	OldUID string
	PeerID string
}

// FilesLs wraps files/ls entries in the Hive Cluster.
type respFilesLs struct {
	Entries []FileLsEntry
}

// map to ipld.Link
type FileLsEntry struct {
	Name string
	Type int
	Size uint64
	Hash string
}

type respFilesStat struct {
	Hash           string
	Size           uint64
	CumulativeSize uint64
	Blocks         int
	Type           string
	WithLocality   bool
	Local          bool
	SizeLocal      uint64
}

type respFilesWrite struct {
	ContentType string
	BodyBuf     *bytes.Buffer
	Params      []string
}

type respNamePublish struct {
	Name  string
	Value string
}

// map to ipld.Link
type respObjectLink struct {
	Hash  string
	Links []ObjectEntry
}

type ObjectEntry struct {
	Name string
	Hash string
	Size uint64
}

// NewConnector creates the component and leaves it ready to be started
func NewConnector(nodeAddr string) (*Connector, error) {
	c := &http.Client{} // timeouts are handled by context timeouts

	ctx, cancel := context.WithCancel(context.Background())

	hive := &Connector{
		ctx: ctx,
		// config: cfg
		cancel:   cancel,
		nodeAddr: nodeAddr,
		// rpcReady: make(chan struct{}, 1),
		client: c,
	}

	go hive.run()
	return hive, nil
}

// connects all ipfs daemons when
// we receive the rpcReady signal.
func (ipfs *Connector) run() {
	// Do not shutdown while launching threads
	// -- prevents race conditions with ipfs.wg.
	ipfs.shutdownLock.Lock()
	defer ipfs.shutdownLock.Unlock()

	// This runs ipfs swarm connect to the daemons of other cluster members
	ipfs.wg.Add(1)
	go func() {
		defer ipfs.wg.Done()

		// It does not hurt to wait a little bit. i.e. think cluster
		// peers which are started at the same time as the ipfs
		// daemon...
		tmr := time.NewTimer(1000 * 60)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			// do not hang this goroutine if this call hangs
			// otherwise we hang during shutdown

		case <-ipfs.ctx.Done():
			return
		}
	}()
}

func (ipfs *Connector) doPostCtx(ctx context.Context, client *http.Client, respRL, path string, contentType string, postBody io.Reader) (*http.Response, error) {
	logger.Info("posting ", path)

	urlstr := fmt.Sprintf("%s/%s", respRL, path)

	req, err := http.NewRequest("POST", urlstr, postBody)
	if err != nil {
		logger.Error("error creating POST request:", err)
	}

	req.Header.Set("Content-Type", contentType)
	req = req.WithContext(ctx)
	res, err := ipfs.client.Do(req)
	if err != nil {
		logger.Error("error posting to IPFS:", err)
	}

	return res, err
}

// checkResponse tries to parse an error message on non StatusOK responses
// from ipfs.
func checkResponse(path string, code int, body []byte) error {
	if code == http.StatusOK {
		return nil
	}

	var ipfsErr ipfsError

	if body != nil && json.Unmarshal(body, &ipfsErr) == nil {
		return fmt.Errorf("IPFS unsuccessful: %d: %s", code, ipfsErr.Message)
	}
	// No error response with useful message from ipfs
	return fmt.Errorf("IPFS-post '%s' unsuccessful: %d: %s", path, code, body)
}

// postCtx makes a POST request against
// the ipfs daemon, reads the full body of the response and
// returns it after checking for errors.
func (ipfs *Connector) postCtx(ctx context.Context, path string, contentType string, postBody io.Reader) ([]byte, error) {
	res, err := ipfs.doPostCtx(ctx, ipfs.client, ipfs.respRL(), path, contentType, postBody)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logger.Errorf("error reading response body: %s", err)
		return nil, err
	}
	return body, checkResponse(path, res.StatusCode, body)
}

// postDiscardBodyCtx makes a POST requests but discards the body
// of the response directly after reading it.
func (ipfs *Connector) postDiscardBodyCtx(ctx context.Context, path string) error {
	res, err := ipfs.doPostCtx(ctx, ipfs.client, ipfs.respRL(), path, "", nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(ioutil.Discard, res.Body)
	if err != nil {
		return err
	}
	return checkResponse(path, res.StatusCode, nil)
}

// respRL is a short-hand for building the url of the IPFS
// daemon API.
func (ipfs *Connector) respRL() string {
	return fmt.Sprintf("http://%s/api/v0", ipfs.nodeAddr)
}

type hiveErrorString struct {
	s string
}

func (e *hiveErrorString) Error() string {
	return e.s
}

func hiveError(err error, uid string) error {
	e := err.Error()
	return &hiveErrorString{strings.Replace(e, "/nodes/"+uid, "", -1)}
}

// create a virtual id.
func (ipfs *Connector) UidNew(name string) (respUIDSecret, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	secret := respUIDSecret{}
	url := "key/gen?arg=" + name + "&type=rsa"
	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	url = "files/mkdir?arg=/nodes/" + name + "&parents=true"
	_, err = ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	var keyGen ipfsKeyGenResp
	err = json.Unmarshal(res, &keyGen)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	secret.UID = keyGen.Name
	secret.PeerID = keyGen.Id

	return secret, nil
}

// log in Hive cluster and get new id
func (ipfs *Connector) UidRenew(l []string) (respUIDRenew, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	secret := respUIDRenew{}
	url := "key/rename?arg=" + l[0] + "&arg=" + l[1]
	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	url = "files/mv?arg=/nodes/" + l[0] + "&arg=/nodes/" + l[1]
	_, err = ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	var keyRename ipfsKeyRenameResp
	err = json.Unmarshal(res, &keyRename)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	secret.UID = keyRename.Now
	secret.OldUID = keyRename.Was
	secret.PeerID = keyRename.Id

	return secret, nil
}

// log in Hive cluster and get new id
func (ipfs *Connector) UidInfo(uid string) (respUIDSecret, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	secret := respUIDSecret{}
	url := "key/list"
	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	var keyList ipfsKeyListResp
	err = json.Unmarshal(res, &keyList)
	if err != nil {
		logger.Error(err)
		return secret, err
	}

	for _, key := range keyList.Keys {
		if key.Name == uid {
			secret.UID = key.Name
			secret.PeerID = key.Id
			break
		}
	}

	return secret, nil
}

// add file
func (ipfs *Connector) FileAdd(fname string, reader io.Reader) (ObjectEntry, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	objectLink := ObjectEntry{}

	url := "file/add?arg=" + fname

	contentType := "application/json"
	res, err := ipfs.postCtx(ctx, url, contentType, reader)
	if err != nil {
		logger.Error(err)
		return objectLink, err
	}

	err = json.Unmarshal(res, &objectLink)
	if err != nil {
		logger.Error(err)
		return objectLink, err
	}

	return objectLink, nil
}

// add file
func (ipfs *Connector) Mkdir() (respObjectLink, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	respObjectLink := respObjectLink{}

	url := "object/new?arg=unixfs-dir"

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return respObjectLink, err
	}

	err = json.Unmarshal(res, &respObjectLink)
	if err != nil {
		logger.Error(err)
		return respObjectLink, err
	}

	return respObjectLink, nil
}

// get file from IPFS service
func (ipfs *Connector) FileGet(path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	url := "file/get?arg=" + path

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return res, nil
}

// copy file to Hive
func (ipfs *Connector) FilesCp(l []string) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "files/cp?arg=" + l[1] + "&arg=" + "/nodes/" + l[0] + l[2]
	_, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// list file or directory
func (ipfs *Connector) FileLs(path string) (respFilesLs, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "file/ls?arg=" + path
	lsrsp := respFilesLs{}

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return lsrsp, err
	}

	err = json.Unmarshal(res, &lsrsp)
	if err != nil {
		logger.Error(err)
		return lsrsp, err
	}

	return lsrsp, nil
}

/*
// file flushs
func (ipfs *Connector) FilesFlush(l []string) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "files/flush?arg=" + "/nodes/" + l[0] + l[1]

	_, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return hiveError(err, l[0])
	}

	return nil
}
*/

// list file or directory
func (ipfs *Connector) FilesLs(uid string, path string) (respFilesLs, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "files/ls?uid=" + uid + "&path=" + path
	lsrsp := respFilesLs{}

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return lsrsp, err
	}

	err = json.Unmarshal(res, &lsrsp)
	if err != nil {
		logger.Error(err)
		return lsrsp, err
	}

	return lsrsp, nil
}

// create a directotry
func (ipfs *Connector) FilesMkdir(uid string, path string, parents bool) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	recursive := "false"
	if parents {
		recursive = "true"
	}

	url := "files/mkdir?uid=" + uid + "&path=" + path + "&parents=" + recursive

	_, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// move files
func (ipfs *Connector) FilesMv(mv []string) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "files/mv?arg=" + "/nodes/" + mv[0] + mv[1] + "&arg=" + "/nodes/" + mv[0] + mv[2]

	_, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return hiveError(err, mv[0])
	}

	return nil
}

// read file
func (ipfs *Connector) FilesRead(uid, path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()
	url := "files/read?uid=" + uid + "&path=" + path
	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return res, nil
}

// remove file
func (ipfs *Connector) FilesRm(uid string, path string, recursive bool) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	_recursive := "false"
	if recursive {
		_recursive = "true"
	}
	url := "files/rm?uid=" + uid + "&path=" + path + "&recursive=" + _recursive

	_, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// get file statistic
func (ipfs *Connector) FilesStat(uid string, path string) (respFilesStat, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	FilesStat := respFilesStat{}
	url := "files/stat?uid=" + uid + "&path=" + path

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return FilesStat, err
	}

	err = json.Unmarshal(res, &FilesStat)
	if err != nil {
		logger.Error(err)
		return FilesStat, err
	}

	return FilesStat, nil
}

// write file
func (ipfs *Connector) FilesWrite(uid string, path string, offset uint64, create bool, truncate bool, count uint64, reader io.Reader) error {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	url := "files/write?uid=" + uid + "&path=" + path
	url = url + fmt.Sprintf("&offset=%d&create=%t&truncate=%t&count=%d", offset, create, truncate, count)

	contentType := ""
	if reader != nil {
		contentType = "multipart/form-data"
	}
	_, err := ipfs.postCtx(ctx, url, contentType, reader)
	if err != nil {
		logger.Error(err)
	}

	return nil
}

// ObjPatchAddlink
func (ipfs *Connector) ObjPatchAddlink(rootHash string, fname string, object string) (respObjectLink, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	respObjectLink := respObjectLink{}
	url := "object/patch/add-link?arg=" + rootHash + "&arg=" + fname + "&arg=" + object

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return respObjectLink, err
	}

	err = json.Unmarshal(res, &respObjectLink)
	if err != nil {
		logger.Error(err)
		return respObjectLink, err
	}

	return respObjectLink, nil
}

// NamePublish publish ipfs path with uid
func (ipfs *Connector) NamePublish(uid string, id string) (respNamePublish, error) {
	ctx, cancel := context.WithTimeout(ipfs.ctx, HiveRequestTimeout)
	defer cancel()

	NamePublish := respNamePublish{}
	url := "name/publish?arg=" + id + "&key=" + uid

	res, err := ipfs.postCtx(ctx, url, "", nil)
	if err != nil {
		logger.Error(err)
		return NamePublish, err
	}

	err = json.Unmarshal(res, &NamePublish)
	if err != nil {
		logger.Error(err)
		return NamePublish, err
	}

	return NamePublish, nil
}
