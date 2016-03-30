package proxy

import (
    "github.com/siddontang/go-mysql/mysql"
    "net"
    "github.com/siddontang/go-mysql/server"
    "github.com/siddontang/go-mysql/client"
    log "github.com/Sirupsen/logrus"
    "time"
    "fmt"
)

type TargetDbSettings struct {
    DbUser string
    DbPassword string
    DbHost string
    DbName string
    DbPort int
}
type MysqlProxyImporter struct {
    conn *client.Conn
    proxyListener net.Listener
    proxyMysqlConn *server.Conn
    statements map[int64] *client.Stmt
    commandCh chan Command
    targetDbSettings *TargetDbSettings

    proxyHost string
    proxyPort int

    state string
}

type Command uint16
const COMMAND_STOP Command = 1

func MakeProxyImporter(host string, port int, settings *TargetDbSettings) (*MysqlProxyImporter, error) {
    targetConn, err := client.Connect(fmt.Sprintf("%v:%v", settings.DbHost, settings.DbPort), settings.DbUser, settings.DbPassword, settings.DbName)

    if err != nil {
        panic(err)
    }

    handler := &MysqlProxyImporter{
        conn: targetConn,
        statements: make(map[int64]*client.Stmt),
        targetDbSettings: settings,
        commandCh: make(chan Command),
        proxyHost: host,
        proxyPort: port,
    }

    return handler, nil
}
func (h *MysqlProxyImporter)Start() error {
    l, err := net.Listen("tcp4", fmt.Sprintf("%v:%v", h.proxyHost, h.proxyPort))
    if err != nil {
        return err
    }

    h.proxyListener = l

    go func() {
        for {
            select {
            case command := <-h.commandCh:
                switch(command) {
                case COMMAND_STOP:
                    log.Debugf("[mysql-proxy] Stopping importer")
                    err := h.stop()

                    if err != nil {
                        log.Errorf("Failed to stop: %v", err)
                    }
                default:
                    log.Errorf("Unknown command %v", command)
                }
            }
        }
    }()

    go func() {
        c, err := l.Accept()
        if err != nil {
            panic(err) // todo handle this
            //return nil, err
        }

        log.Infof("ACCEPT: user: %v; password: %v", h.targetDbSettings.DbUser, h.targetDbSettings.DbPassword)
        proxyConn, err := server.NewConn(c, h.targetDbSettings.DbUser, h.targetDbSettings.DbPassword, h)
        if err != nil {
            panic(err) // todo handle this
        }

        h.proxyMysqlConn = proxyConn

        for {
            err := proxyConn.HandleCommand()

            if err != nil {
                if h.state == "stopped" || h.state == "stopping" {
                    log.Infof("[mysql-proxy] Stopping handle mysql command")
                } else {
                    log.Warnf("[mysql-proxy] Handle command err: %v", err)
                }

                break
            }
        }

        l.Close()
    }()

    return nil
}
func (h *MysqlProxyImporter)SendCommand(command Command) {
    h.commandCh <- command
}
func (h *MysqlProxyImporter)stop() error {
    h.state = "stopping"
    defer func() { h.state = "stopped"}()

    if err := h.proxyListener.Close(); err != nil {
        return err
    }

    h.proxyMysqlConn.Close()

    if err := h.conn.Close(); err != nil {
        return err
    }

    return nil
}
func (h MysqlProxyImporter)UseDB(dbName string) error {
    return h.conn.UseDB(dbName)
}
func (h MysqlProxyImporter)HandleQuery(query string) (*mysql.Result, error) {
    res, err := h.conn.Execute(query)

    if err != nil {
        log.Errorf("Error in query: %v; original query was: %s", err, query)
    }

    return res, err
}

func (h MysqlProxyImporter)HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
    return h.conn.FieldList(table, fieldWildcard)
}
func (h MysqlProxyImporter)HandleStmtPrepare(query string) (int, int, interface{}, error) {
    stmt, err := h.conn.Prepare(query)

    if err != nil {
        log.Errorf("Error in prepare: %v", err)
        return 0, 0, nil, err
    }

    id := time.Now().UnixNano()
    h.statements[id] = stmt

    paramNum := stmt.ParamNum()
    colNum := stmt.ColumnNum()

    log.Debugf("Statement: param: %v; col: %v", paramNum, colNum)

    return paramNum, colNum, id, nil
}
func (h MysqlProxyImporter)HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
    if intContext, ok := context.(int64); ok {
        if stmt, ok := h.statements[intContext]; !ok {
            log.Warnf("Creating statement on-the-fly and execute it")
            inlineStmt, err := h.conn.Prepare(query)

            if err != nil {
                return nil, err
            }

            return inlineStmt.Execute(args...)
        } else {
            return stmt.Execute(args...)
        }
    } else {
        log.Errorf("Invalid context: %+v", context)
        return nil, fmt.Errorf("Invalid context")
    }
}
func (h MysqlProxyImporter)HandleStmtClose(context interface{}) error {
    intContext, ok := context.(int64)
    if !ok {
        return nil
    }

    stmt, ok := h.statements[intContext]
    if !ok {
        return nil
    }

    log.Debugf("HandleStmtClose: %v", intContext)
    return stmt.Close()
}