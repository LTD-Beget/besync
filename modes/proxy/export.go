package proxy

import (
    log "github.com/Sirupsen/logrus"
    "github.com/LTD-Beget/besync/inspector"
    "github.com/LTD-Beget/besync/modes/proxy/tableChunk"
    "time"
    "database/sql"
    "github.com/go-sql-driver/mysql"
    "fmt"
    "encoding/json"
    "sync"
    "github.com/jeffail/tunny"
    "github.com/hashicorp/go-version"
    "strings"
)

type exporter struct {
    settings           *Settings
    dumpId             int64

    schema             *Schema
    sourceDb           *sql.DB
    targetDb           *sql.DB
    inspector          inspector.Inspector
    tableColumns       map[string]map[string]*inspector.Column
    proxyInfo          *ProxyStartResponse
    workPool           *tunny.WorkPool
    sourceMysqlVersion *version.Version
}

type DbSettings struct {
    Name     string
    Host     string
    Port     int
    User     string
    Password string
}

type ProxySettings struct {
    Host       string
    Port       int
    ListenAddr string
}

type ExportSettings struct {
    MaxRowsPerStatement   int             // Максимальное количество строк, которое может быть вставлено с помощью одного инсерта. 0 - неограниченно
    WorkersCount          int             // Максимальное количество воркеров, выполняющих экспорт
    WithoutProxy          bool            // Не использовать прокси. В этом случае сразу подключаемся к targetDb
    MaxWorkersOnLastTable int             // Максимальное количество воркеров на последней таблице
    TableChunkSize        int64

    AddDropTrigger        bool            // Add DROP TRIGGER IF EXISTS before CREATE ANY TRIGGER
    AddDropTable          bool            // Add DROP TABLE statement before each CREATE TABLE statement
    AddDropProcedure      bool            // add DROP PROCEDURE statement before dump each procedure
    NoCreateTable         bool            //? Do not write CREATE TABLE statements that re-create each dumped table

    NoData                bool            // Do not dump table contents
    IncludeTables         []string        // list of tables/views to dump. If empty, all tables was processed
    ExcludeTables         []string        // list of tables/views to exclude from dump. If empty, no tables was excluded
    ExcludeTriggers       []string        // list of triggers to exclude from dump. If empty, no triggers was excluded
    NoViews               bool            // Do not dump views structure (n/u)
    NoProcedures          bool            // Do not dump any procedures (n/u)
    NoLockTables          bool
    NoTransaction         bool
}

// Exporter settings
type Settings struct {
    SourceDb *DbSettings
    TargetDb *DbSettings
    Proxy    *ProxySettings
    Export   *ExportSettings
}

type Schema struct {
    Tables       []string
    Views        []string
    Triggers     []string
    Procedures   []string
    TableColumns map[string]map[string]*inspector.Column
}

func MakeExporter(exportSettings *Settings) *exporter {
    server := &exporter{
        settings: exportSettings,
        dumpId: time.Now().UnixNano(),
        tableColumns: make(map[string]map[string]*inspector.Column),
        schema: &Schema{
            TableColumns: make(map[string]map[string]*inspector.Column),
        },
    }

    return server
}
func (s *exporter)Start() error {
    defer s.endDump() // for graceful shutdown
    s.startDump()

    return nil
}
func (s *exporter)startDump() {
    db, err := s.newSourceDbConnection()

    if err != nil {
        log.Panic(err)
    }

    s.sourceDb = db

    if err := s.determineSourceMysqlVersion(); err != nil {
        log.Panic(err)
    }

    s.inspector = inspector.MakeMysqlInspector(db, s.sourceMysqlVersion)

    if err := s.loadSchema(); err != nil {
        log.Panic(err)
    }

    if err := s.prepareProxy(); err != nil {
        log.Panic(err)
    }

    if err := s.lockAllTables(); err != nil {
        log.Panic(err)
    }

    s.workPool, err = s.createWorkerPool()
    if err != nil {
        log.Panic(err)
    }
    defer s.workPool.Close()

    if err := s.unlockAllTables(); err != nil {
        log.Panic(err)
    }

    if err := s.exportTables(); err != nil {
        log.Panic(err)
    }

    if err := s.exportViews(); err != nil {
        log.Panic(err)
    }

    if err := s.exportRoutines(); err != nil {
        log.Panic(err)
    }
}
func (s *exporter)newSourceDbConnection() (*sql.DB, error) {
    mysqlConfig := &mysql.Config{
        User: s.settings.SourceDb.User,
        Passwd: s.settings.SourceDb.Password,
        Addr: fmt.Sprintf("%s:%v", s.settings.SourceDb.Host, s.settings.SourceDb.Port),
        Net: "tcp",
        DBName: s.settings.SourceDb.Name,
        Params: map[string]string{
            "charset": "binary",
        },
    }

    db, err := sql.Open("mysql", mysqlConfig.FormatDSN())
    db.SetMaxIdleConns(1)
    db.SetMaxOpenConns(1)

    return db, err
}
func (s *exporter)determineSourceMysqlVersion() error {
    row := s.sourceDb.QueryRow("SELECT VERSION()")

    var mysqlVersion string
    err := row.Scan(&mysqlVersion)
    if err != nil {
        return err
    }

    s.sourceMysqlVersion, err = version.NewVersion(mysqlVersion)
    if err != nil {
        return err
    }

    return nil
}
func (s *exporter)exportTables() error {
    var wgSchema sync.WaitGroup = sync.WaitGroup{}

    maxOnLast := s.settings.Export.MaxWorkersOnLastTable
    if maxOnLast < 1 {
        maxOnLast = 1
    }

    cm := tableChunk.MakeManager(s.settings.Export.WorkersCount, maxOnLast)

    // create tables first
    tablesToDump := make([]string, 0)
    for _, tableName := range s.schema.Tables {
        columns, err := s.inspector.ColumnTypes(tableName)
        if err != nil {
            return err
        }

        s.schema.TableColumns[tableName] = columns
        log.Debugf("[export] Inspected %v columns: %+v", tableName, columns)

        tablesToDump = append(tablesToDump, tableName)

        if !s.settings.Export.NoData {
            chunks, err := tableChunk.CalculateChunksForTable(tableName, s.settings.Export.TableChunkSize, s.inspector)
            if err != nil {
                return err
            }

            for _, chunk := range chunks {
                cm.AddChunk(chunk)
            }
        }

        wgSchema.Add(1)

        s.workPool.SendWorkAsync(&jobCreateTable{
            tableName: tableName,
            withDropTable: s.settings.Export.AddDropTable,
        }, func(result interface{}, err error) {
            if resultErr, ok := result.(error); ok || err != nil {
                panic(fmt.Sprintf("[export] create table worker error: %v%v", resultErr, err))
            }

            wgSchema.Done()
        })

        wgSchema.Wait()
    }

    if s.settings.Export.NoData {
        log.Infof("[export] NoData = true; Skipping table data dump")
        return nil
    }

    var wgData sync.WaitGroup = sync.WaitGroup{}

    for {
        chunk := cm.GetNext()
        if chunk == nil {
            break
        }

        contextLogger := log.WithField("processing now", cm.GetProcessing())
        contextLogger.Debugf("[export] got chunk: %+v", chunk)

        wgData.Add(1)

        log.Debugf("[export] TABLE [%v] CHUNK IS %+v", chunk.TableName, chunk)
        s.workPool.SendWorkAsync(&jobExportTable{
            tableName: chunk.TableName,
            condition: chunk.Condition,
            columnInfo: s.schema.TableColumns[chunk.TableName],
            rowsPerStmt: s.settings.Export.MaxRowsPerStatement,
        }, func(tableName string) func(result interface{}, err error) {
            return func(result interface{}, err error) {
                if resultErr, ok := result.(error); ok || err != nil {
                    panic(fmt.Sprintf("[export][%s] export table worker error: %v%v", chunk.TableName, resultErr, err))
                }

                cm.Done(tableName)
                wgData.Done()
            }
        }(chunk.TableName))
    }

    log.Debugf("[export] Waiting for table data export")
    wgData.Wait()
    log.Infof("[export] All tables was exported")

    return nil
}
func (s *exporter)exportViews() error {
    var wgTableViews sync.WaitGroup = sync.WaitGroup{}

    viewsToCreate := make([]string, 0)
    for _, viewName := range s.schema.Views {
        columns, err := s.inspector.ColumnTypes(viewName)
        if err != nil {
            return err
        }

        s.schema.TableColumns[viewName] = columns
        viewsToCreate = append(viewsToCreate, viewName)

        wgTableViews.Add(1)

        s.workPool.SendWorkAsync(&jobCreateViewTable{
            viewName: viewName,
            withDropView: s.settings.Export.AddDropTable,
            columnInfo: columns,
        }, func(result interface{}, err error) {
            if resultErr, ok := result.(error); ok || err != nil {
                panic(fmt.Sprintf("[export] create view worker error: %v%v", resultErr, err))
            }

            wgTableViews.Done()
        })

        wgTableViews.Wait()
    }

    //log.Debugf("[export] Waiting for view table create")
    //wgTableViews.Wait()
    //log.Infof("[export] All view tables was exported")

    var wgViews sync.WaitGroup = sync.WaitGroup{}
    for _, viewName := range viewsToCreate {
        wgViews.Add(1)

        s.workPool.SendWorkAsync(&jobCreateView{
            viewName: viewName,
        }, func(result interface{}, err error) {
            if resultErr, ok := result.(error); ok || err != nil {
                panic(fmt.Sprintf("[export] create view worker error: %v%v", resultErr, err))
            }

            wgViews.Done()
        })

        wgViews.Wait()
    }

    return nil
}
func (s *exporter)exportRoutines() error {
    var wgTriggers sync.WaitGroup = sync.WaitGroup{}

    for _, triggerName := range s.schema.Triggers {
        wgTriggers.Add(1)

        s.workPool.SendWorkAsync(&jobCreateTrigger{
            triggerName: triggerName,
            withDropTrigger: s.settings.Export.AddDropTrigger,
        }, func(result interface{}, err error) {
            if result != nil {
                log.Errorf("[export][create trigger] Error: %v", result)
            }
            wgTriggers.Done()
        })

        wgTriggers.Wait()
    }

    var wgProcedures sync.WaitGroup = sync.WaitGroup{}

    for _, procName := range s.schema.Procedures {
        wgProcedures.Add(1)
        s.workPool.SendWorkAsync(&jobCreateProcedure{
            procName: procName,
            withDropProcedure: s.settings.Export.AddDropProcedure,
        }, func(result interface{}, err error) {
            if resultErr, ok := result.(error); ok || err != nil {
                panic(fmt.Sprintf("[export] create routine worker error: %v%v", resultErr, err))
            }

            wgProcedures.Done()
        })

        wgProcedures.Wait()
    }

    return nil
}
func (s *exporter)createWorkerPool() (*tunny.WorkPool, error) {
    var host string
    var ports []int

    if s.settings.Export.WithoutProxy {
        host = s.settings.TargetDb.Host

        ports = make([]int, s.settings.Export.WorkersCount)
        for i, _ := range ports {
            ports[i] = s.settings.TargetDb.Port
        }
    } else {
        host = s.settings.Proxy.ListenAddr
        ports = s.proxyInfo.Ports
    }

    log.Debugf("[export] Creating worker pool with %v workers", s.settings.Export.WorkersCount)

    workers := make([]tunny.TunnyWorker, s.settings.Export.WorkersCount)
    for i, _ := range workers {
        workerSourceDb, err := s.newSourceDbConnection()
        if err != nil {
            return nil, err
        }

        worker, err := MakeWorker(workerSourceDb, s.sourceMysqlVersion, !s.settings.Export.NoTransaction,
            &DbSettings{
                Name: s.settings.TargetDb.Name,
                Host: host,
                Port: ports[i],
                User: s.settings.TargetDb.User,
                Password: s.settings.TargetDb.Password,
            },
        )

        if err != nil {
            return nil, err
        }

        workers[i] = worker
    }

    pool, err := tunny.CreateCustomPool(workers).Open()
    if err != nil {
        return nil, err
    }

    return pool, nil
}
func (s *exporter)lockAllTables() error {
    if s.settings.Export.NoLockTables {
        log.Debugf("[export] No lock tables because settings")
        return nil
    }

    tableNames := make([]string, len(s.schema.Tables))
    for i, tableName := range s.schema.Tables {
        tableNames[i] = fmt.Sprintf("`%s` READ LOCAL", tableName)
    }

    log.Infof("[export] Locking all tables")

    sql := fmt.Sprintf("LOCK TABLES %s", strings.Join(tableNames, ","))
    log.Infof("SQL: %s", sql)
    if _, err := s.sourceDb.Exec(sql); err != nil {
        return err
    }

    return nil
}
func (s *exporter)unlockAllTables() error {
    if s.settings.Export.NoLockTables {
        log.Debugf("[export] No unlock table needed because settings")
        return nil
    }

    if _, err := s.sourceDb.Exec("UNLOCK TABLES"); err != nil {
        return err
    }

    return nil
}
func (s *exporter)loadSchema() error {
    // TABLES
    tables, err := s.inspector.Tables(s.settings.SourceDb.Name)
    if err != nil {
        return err
    }
    for _, tableName := range tables {
        if inSlice(s.settings.Export.ExcludeTables, tableName) {
            log.Debugf("[export] Table %v marked as excluded. Skipping...", tableName)
            continue
        }

        if len(s.settings.Export.IncludeTables) > 0 {
            if !inSlice(s.settings.Export.IncludeTables, tableName) {
                log.Debugf("[export] Table %v not included in dump. Skipping...", tableName)
                continue
            }
        }

        s.schema.Tables = append(s.schema.Tables, tableName)
    }
    log.Infof("[export] Inspected database tables: %+v", s.schema.Tables)


    // VIEWS
    views, err := s.inspector.Views(s.settings.SourceDb.Name)
    if err != nil {
        return err
    }
    for _, viewName := range views {
        if inSlice(s.settings.Export.ExcludeTables, viewName) {
            log.Debugf("[export] Table %v marked as excluded. Skipping...", viewName)
            continue
        }

        if len(s.settings.Export.IncludeTables) > 0 {
            if !inSlice(s.settings.Export.IncludeTables, viewName) {
                log.Debugf("[export] Table %v not included in dump. Skipping...", viewName)
                continue
            }
        }

        s.schema.Views = append(s.schema.Views, viewName)
    }
    log.Infof("[export] Inspected database views: %+v", s.schema.Views)


    // TRIGGERS
    triggers, err := s.inspector.Triggers(s.settings.SourceDb.Name)
    if err != nil {
        return err
    }
    for _, triggerName := range triggers {
        if inSlice(s.settings.Export.ExcludeTriggers, triggerName) {
            log.Debugf("[export] Trigger %v marked as excluded. Skipping...", triggerName)
            continue
        }

        s.schema.Triggers = append(s.schema.Triggers, triggerName)
    }
    log.Infof("[export] Inspected database triggers: %+v", s.schema.Triggers)


    // PROCEDURES
    procedures, err := s.inspector.Procedures(s.settings.SourceDb.Name)
    if err != nil {
        return err
    }
    s.schema.Procedures = procedures
    log.Infof("[export] Inspected database procedures: %+v", s.schema.Procedures)

    return nil
}

func (s *exporter)prepareProxy() error {
    if s.settings.Export.WithoutProxy {
        log.Infof("[export] We don't start proxy because settings")
        return nil
    }

    proxyInfo, err := s.startProxy()
    if err != nil {
        return err
    }

    log.Infof("[export] Got proxy info: %+v", proxyInfo)
    s.proxyInfo = proxyInfo

    return nil
}
func (s *exporter)endDump() {
    if s.proxyInfo != nil && s.proxyInfo.Id != 0 {
        log.Infof("[export] Stopping proxy importer %v", s.proxyInfo.Id)

        host := fmt.Sprintf("http://%s:%v", s.settings.Proxy.Host, s.settings.Proxy.Port)

        _, err := httpRequest("DELETE", host, fmt.Sprintf("/proxy/%v/stop", s.proxyInfo.Id), nil)
        if err != nil {
            log.Errorf("[export] ", err.Error())
        }
    }
}
func (s *exporter)startProxy() (*ProxyStartResponse, error) {
    request := &ProxyStartRequest{
        DbHost: s.settings.TargetDb.Host,
        DbPort: s.settings.TargetDb.Port,
        DbPassword: s.settings.TargetDb.Password,
        DbUser: s.settings.TargetDb.User,
        DbName: s.settings.TargetDb.Name,
        MysqlListenAddr: s.settings.Proxy.ListenAddr,
        Count: s.settings.Export.WorkersCount,
    }

    host := fmt.Sprintf("http://%s:%v", s.settings.Proxy.Host, s.settings.Proxy.Port)
    body, err := httpRequest("POST", host, "/proxy/start", request)
    if err != nil {
        return nil, err
    }

    var proxyResponse = &ProxyStartResponse{}

    if err := json.Unmarshal(body, proxyResponse); err != nil {
        return nil, err
    }

    return proxyResponse, nil
}

func inSlice(slice []string, needle string) bool {
    for _, v := range slice {
        if v == needle {
            return true
        }
    }

    return false
}