package proxy

import (
    "github.com/gorilla/mux"
    "net/http"
    "fmt"
    log "github.com/Sirupsen/logrus"
    "encoding/json"
    "net"
    "io/ioutil"
    "sync"
    "time"
    "strconv"
    "bytes"
)

func Serve(host string, port int) error {
    go logExportStatus()

    r := mux.NewRouter()

    r.HandleFunc("/proxy/start", jsonAction(proxyStartAction)).Methods("POST")
    r.HandleFunc("/proxy/{proxyId}/stop", jsonAction(proxyStopAction)).Methods("DELETE")
    r.HandleFunc("/proxy", jsonAction(proxyListAction)).Methods("GET")

    r.HandleFunc("/sync/start", jsonAction(syncStartAction)).Methods("POST")
    r.HandleFunc("/sync/{syncId}", jsonAction(syncStatusAction)).Methods("GET")

    http.Handle("/", r)

    if err := http.ListenAndServe(fmt.Sprintf("%v:%v", host, port), r); err != nil {
        return err
    }

    return nil
}

type resultError struct {
    Error string
}

func jsonAction(f func(*http.Request) (interface{}, error)) (func(http.ResponseWriter, *http.Request)) {
    return func(w http.ResponseWriter, r *http.Request) {
        // Panic handler
        defer func() {
            if r := recover(); r != nil {
                log.Errorf("[http] Internal error: %v", r)

                w.WriteHeader(http.StatusInternalServerError)
                w.Write([]byte("Internal error. See logs for details"))
            }
        }()

        log.Debugf("[http] Got request %v", r.URL)

        w.Header().Set("Content-Type", "application/json")
        result, err := f(r)

        if err != nil {
            log.Errorf("[http] Action %v return error: %v", r.URL, err)
            w.WriteHeader(http.StatusInternalServerError)
            w.Write([]byte(err.Error()))
            return
        }

        if resultErr, ok := result.(error); ok {
            jerr := resultError{Error: resultErr.Error()}

            json, err := json.Marshal(jerr)
            if err != nil {
                w.WriteHeader(http.StatusInternalServerError)
                w.Write([]byte(err.Error()))
                return
            }

            w.Write(json)
            return
        }

        j, err := json.Marshal(result)
        if err != nil {
            log.Errorf("[http] Json marshal error: %v", err)
            w.WriteHeader(http.StatusInternalServerError)
            w.Write([]byte(err.Error()))
            return
        }

        log.Debugf("[http] Return answer: %s", j)
        w.Write(j)
    }
}

var proxyMapMutex = &sync.Mutex{}
var proxyMap = make(map[int64][]*MysqlProxyImporter)

type ProxyStartRequest struct {
    DbHost          string
    DbPort          int
    DbName          string
    DbUser          string
    DbPassword      string

    Count           int    // Требуемое количество подключений
    MysqlListenAddr string // ip-адрес, на котором будет слушать mysql-proxy
}
func (r *ProxyStartRequest)validate() error {
    if r.DbHost == "" {
        return fmt.Errorf("DbHost cannot be blank")
    }
    if r.DbPort == 0 {
        return fmt.Errorf("DbPort cannot be blank")
    }
    if r.Count == 0 {
        return fmt.Errorf("Count cannot be blank")
    }
    if r.DbName == "" {
        return fmt.Errorf("DbName cannot be blank")
    }
    if r.DbUser == "" {
        return fmt.Errorf("DbUser cannot be blank")
    }
    if r.DbPassword == "" {
        return fmt.Errorf("DbPassword cannot be blank")
    }
    if r.MysqlListenAddr == "" {
        return fmt.Errorf("MysqlListenAddr cannot be blank")
    }

    return nil
}
type ProxyStartResponse struct {
    Id int64
    Ports []int
}

func proxyStartAction(r *http.Request) (interface{}, error) {
    var m ProxyStartRequest
    b, _ := ioutil.ReadAll(r.Body)

    if err := json.Unmarshal(b, &m); err != nil {
        return nil, err
    }
    if err := m.validate(); err != nil {
        return nil, err
    }

    id := time.Now().UnixNano()
    ports := make([]int, m.Count)

    for i := 0; i < m.Count; i++ {
        port, err := getPort(m.MysqlListenAddr)
        if err != nil {
            return nil, err
        }

        mysqlProxy, err := MakeProxyImporter(m.MysqlListenAddr, port, &TargetDbSettings{
            DbUser: m.DbUser,
            DbPassword: m.DbPassword,
            DbHost: m.DbHost,
            DbName: m.DbName,
            DbPort: m.DbPort,
        })

        if err != nil {
            return nil, err
        }

        if err := mysqlProxy.Start(); err != nil {
            return nil, err
        }

        proxyMapMutex.Lock()
        proxyMap[id] = append(proxyMap[id], mysqlProxy)
        proxyMapMutex.Unlock()

        ports[i] = port
    }

    return &ProxyStartResponse{
        Id: id,
        Ports: ports,
    }, nil
}

type StopProxyResponse struct {
    Ok int64
}
func proxyStopAction(r *http.Request) (interface{}, error) {
    vars := mux.Vars(r)
    proxyId, err := strconv.ParseInt(vars["proxyId"], 10, 64)
    if err != nil {
        return nil, err
    }

    proxyMapMutex.Lock()
    defer proxyMapMutex.Unlock()

    proxyMysqlList, ok := proxyMap[proxyId]
    if !ok {
       return nil, fmt.Errorf("Cannot find proxies with id %v", proxyId)
    }

    log.Infof("Stopping proxy %v", proxyId)

    for _, proxyMysql := range proxyMysqlList {
        proxyMysql.SendCommand(COMMAND_STOP)
    }

    delete(proxyMap, proxyId)

    return &StopProxyResponse{Ok: proxyId}, nil
}

type ProxyListItem struct {
    Id int64
    Ports []int
    DbName string
}
type ProxyListResponse []ProxyListItem
func proxyListAction(r *http.Request) (interface{}, error) {
    proxyMapMutex.Lock()
    defer proxyMapMutex.Unlock()

    proxyList := make(ProxyListResponse, len(proxyMap))

    i := 0
    for id, mysqlProxyList := range proxyMap {
        proxyPorts := make([]int, len(mysqlProxyList))
        for i, mysqlProxy := range mysqlProxyList {
            proxyPorts[i] = mysqlProxy.proxyPort
        }

        proxyDbName := mysqlProxyList[0].targetDbSettings.DbName

        proxyList[i] = ProxyListItem{
            Id: id,
            Ports: proxyPorts,
            DbName: proxyDbName,
        }

        i += 1
    }

    return proxyList, nil
}

type SyncStartRequest struct {
    Settings
}
func (s *SyncStartRequest)validate() error {
    if s.SourceDb == nil {
        return fmt.Errorf("'TargetDb' options is required")
    }
    if s.TargetDb == nil {
        return fmt.Errorf("'TargetDb' options is required")
    }
    if s.Proxy == nil {
        return fmt.Errorf("'Proxy' options is required")
    }
    if s.Export == nil {
        return fmt.Errorf("'Export' options is required")
    }

    return nil
}

var exportStatusCh = make(chan *ExportStatus, 50)
func logExportStatus() {
    for status := range exportStatusCh {
        log.Infof("[export #%v]: %v:%v", status.Id, status.Status, status.Error)
    }
}

type SyncStartResponse struct {
    Id int64
}
func syncStartAction(r *http.Request) (interface{}, error) {
    var s SyncStartRequest
    b, _ := ioutil.ReadAll(r.Body)

    if err := json.Unmarshal(b, &s); err != nil {
        return nil, err
    }
    if err := s.validate(); err != nil {
        return nil, err
    }

    id, err := Manager.StartDump(&s.Settings, exportStatusCh)
    if err != nil {
        return nil, err
    }

    return &SyncStartResponse{Id: id}, nil
}

type SyncStatusResponse struct {
    Id int64
    Status string
    Error string
}
func syncStatusAction(r *http.Request) (interface{}, error) {
    vars := mux.Vars(r)
    syncId, err := strconv.ParseInt(vars["syncId"], 10, 64)
    if err != nil {
        return nil, err
    }

    status, err := Manager.GetStatus(syncId)
    if err != nil {
        return nil, err
    }

    if status == nil {
        return fmt.Errorf("Sync with id %v not found", syncId), nil
    }

    var statusErr string
    if status.Error != nil {
        statusErr = status.Error.Error()
    }

    return &SyncStatusResponse{
        Id: status.Id,
        Status: status.Status,
        Error: statusErr,
    }, nil
}

// Ask the kernel for a free open port that is ready to use
func getPort(ip string) (int, error) {
    addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%v:0", ip))
    if err != nil {
        return 0, err
    }

    l, err := net.ListenTCP("tcp", addr)
    if err != nil {
        return 0, err
    }

    defer l.Close()

    return l.Addr().(*net.TCPAddr).Port, nil
}

func httpRequest(method, host, route string, body interface{}) ([]byte, error) {
    var j []byte

    if body != nil {
        var err error
        j, err = json.Marshal(body)

        if err != nil {
            return nil, err
        }
    }

    req, err := http.NewRequest(method, fmt.Sprintf("%v%v", host, route), bytes.NewReader(j))
    if err != nil {
        return nil, err
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    respBody, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    if resp.StatusCode == http.StatusInternalServerError {
        return nil, fmt.Errorf("[http] Got error: %v", respBody)
    }

    return respBody, nil
}