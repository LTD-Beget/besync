package proxy

import (
    "database/sql"
    "fmt"
    "github.com/go-sql-driver/mysql"
    "github.com/LTD-Beget/besync/inspector"
    log "github.com/Sirupsen/logrus"
    "strconv"
    "strings"
    "github.com/hashicorp/go-version"
)

type jobCreateTable struct {
    tableName     string
    withDropTable bool
}
type jobCreateViewTable struct {
    viewName     string
    withDropView bool
    columnInfo   map[string]*inspector.Column
}
type jobCreateView struct {
    viewName string
}
type jobExportTable struct {
    tableName   string
    condition   string
    columnInfo  map[string]*inspector.Column
    rowsPerStmt int
}
type jobCreateTrigger struct {
    triggerName     string
    withDropTrigger bool
}
type jobCreateProcedure struct {
    procName string
    withDropProcedure bool
}

type worker struct {
    inspector          inspector.Inspector
    sourceDb           *sql.DB
    targetDb           *sql.DB
    targetMysqlVersion *version.Version
    sourceMysqlVersion *version.Version
    withTransaction    bool
    maxAllowedPacket   int64
}

func MakeWorker(sourceDb *sql.DB, sourceMysqlVersion *version.Version, withTransaction bool, targetDbSettings *DbSettings) (*worker, error) {
    mysqlConfig := &mysql.Config{
        User: targetDbSettings.User,
        Passwd: targetDbSettings.Password,
        Addr: fmt.Sprintf("%s:%v", targetDbSettings.Host, targetDbSettings.Port),
        Net: "tcp",
        DBName: targetDbSettings.Name,
        Params: map[string]string{
            "charset": "binary",
            "UNIQUE_CHECKS": "0",
            "FOREIGN_KEY_CHECKS": "0",
            "WAIT_TIMEOUT": "2147483",
            "SESSION sql_MODE": "'ALLOW_INVALID_DATES,NO_AUTO_VALUE_ON_ZERO'",
        },
    }

    targetDb, err := sql.Open("mysql", mysqlConfig.FormatDSN())
    if err != nil {
        return nil, err
    }

    // starting transaction
    if withTransaction {
        transactionSupportVersion, err := version.NewVersion("4.0")
        if err != nil {
            return nil, err
        }

        if sourceMysqlVersion.LessThan(transactionSupportVersion) {
            log.Infof("[worker] Cannot start transaction, because source mysql version lower than 4 (current version is %s)", sourceMysqlVersion)
        } else {
            if _, err := sourceDb.Exec("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
                return nil, err
            }

            if _, err := sourceDb.Exec("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */"); err != nil {
                return nil, err
            }
        }
    } else {
        log.Infof("[worker] We do not start transaction because settings")
    }

    // get max allowed packet
    packetRow := targetDb.QueryRow("SELECT @@max_allowed_packet")

    var maxAllowedPacketByte []byte
    if err := packetRow.Scan(&maxAllowedPacketByte); err != nil {
        return nil, err
    }
    if maxAllowedPacketByte == nil {
        return nil, fmt.Errorf("[worker] @@max_allowed_packet is null!")
    }

    maxAllowedPacket, err := strconv.ParseFloat(string(maxAllowedPacketByte), 64)
    if err != nil {
        return nil, fmt.Errorf("[worker] Failed to parse max_allowed_packet as float: %v", maxAllowedPacketByte)
    }

    inspector := inspector.MakeMysqlInspector(sourceDb, sourceMysqlVersion)
    w := &worker{
        inspector: inspector,
        sourceDb: sourceDb,
        targetDb: targetDb,
        sourceMysqlVersion: sourceMysqlVersion,
        withTransaction: withTransaction,
        maxAllowedPacket: int64(maxAllowedPacket * .9),
    }

    // determine target mysql version
    row := w.targetDb.QueryRow("SELECT VERSION()")

    var mysqlVersion string
    if err := row.Scan(&mysqlVersion); err != nil {
        return nil, err
    }

    w.targetMysqlVersion, err = version.NewVersion(mysqlVersion)
    if err != nil {
        return nil, err
    }

    return w, nil
}
// Use this call to block further jobs if necessary
func (w *worker) TunnyReady() bool {
    return true
}
// This is where the work actually happens
func (w *worker) TunnyJob(job interface{}) interface{} {
    log.Debugf("[worker] Got work %+v", job)

    var err error

    switch job.(type) {
    case *jobCreateTable:
        err = w.createTable(job.(*jobCreateTable))
    case *jobExportTable:
        err = w.exportTable(job.(*jobExportTable))
    case *jobCreateViewTable:
        err = w.createViewTable(job.(*jobCreateViewTable))
    case *jobCreateView:
        err = w.createView(job.(*jobCreateView))
    case *jobCreateTrigger:
        err = w.createTrigger(job.(*jobCreateTrigger))
    case *jobCreateProcedure:
        err = w.createProcedure(job.(*jobCreateProcedure))
    default:
        err = fmt.Errorf("Unknown job %+b given", job)
    }

    return err
}
func (w *worker)TunnyInitialize() {}
func (w *worker)TunnyTerminate() {
    if !w.withTransaction {
        log.Debug("[worker] We don't need commit transaction because settings")
    }

    log.Debugf("[worker] Committing transaction")
    if _, err := w.sourceDb.Exec("COMMIT"); err != nil {
        log.Errorf("[woker] %v", err)
    }
}
func (w *worker) createTable(job *jobCreateTable) error {
    createTableQuery, err := w.inspector.ShowCreateTable(job.tableName)
    if err != nil {
        return err
    }

    if job.withDropTable {
        _, err = w.targetDb.Exec(w.inspector.DropTableQuery(job.tableName))
        if err != nil {
            return err
        }
    }

    log.Infof("[worker] CREATE TABLE: [%s]", createTableQuery)

    _, err = w.targetDb.Exec(createTableQuery)
    if err != nil {
        return err
    }

    return nil
}
// In this job we'r creating tables instead of views and saving name of views
// In job createView we should drop view-table and create real view
func (w *worker) createViewTable(job *jobCreateViewTable) error {
    if job.withDropView {
        if err := w.dropView(job.viewName); err != nil {
            return err
        }
    }

    log.Debugf("[worker] Creating view %v as table to resolve dependencies & saving view name", job.viewName)
    createTableSql := w.inspector.MakeCreateTableQuery(job.viewName, job.columnInfo)

    if _, err := w.targetDb.Exec(createTableSql); err != nil {
        return err
    }

    return nil
}
func (w *worker) createView(job *jobCreateView) error {
    if err := w.dropView(job.viewName); err != nil {
        return err
    }

    createViewSql, err := w.inspector.ShowCreateView(job.viewName)
    if err != nil {
        return err
    }

    if _, err := w.targetDb.Exec(createViewSql); err != nil {
        return err
    }

    log.Infof("[worker] Processed view: %s", job.viewName)

    return nil
}
func (w *worker)createTrigger(job *jobCreateTrigger) error {
    createTriggerQuery, err := w.inspector.ShowCreateTrigger(job.triggerName)
    if err != nil {
        return err
    }

    if job.withDropTrigger {
        if _, err := w.targetDb.Exec(w.inspector.DropTriggerQuery(job.triggerName)); err != nil {
            return err
        }
    }

    if _, err = w.targetDb.Exec(createTriggerQuery); err != nil {
        return err
    }

    log.Infof("[worker] Processed trigger: %v", job.triggerName)

    return nil
}
// todo character set
func (w *worker)createProcedure(job *jobCreateProcedure) error {
    procedureSupportVersion, _ := version.NewVersion("5.0")
    if w.targetMysqlVersion.LessThan(procedureSupportVersion) {
        log.Warnf("[worker] Target mysql version %v is lower than 5. Stored Procedures is not supported. Skipping procedure '%s'", w.targetMysqlVersion, job.procName)
        return nil
    }

    if job.withDropProcedure {
        log.Debugf("[worker] Drop procedure `%s`", job.procName)

        if _, err := w.targetDb.Exec(w.inspector.DropProcedureQuery(job.procName)); err != nil {
            return err
        }
    }

    createProcSql, err := w.inspector.ShowCreateProcedure(job.procName)
    if err != nil {
        return err
    }

    if _, err := w.targetDb.Exec(createProcSql); err != nil {
        return err
    }

    log.Infof("[worker] Processed procedure: %s", job.procName)

    return nil
}
func (w *worker)dropView(viewName string) error {
    if _, err := w.targetDb.Exec(w.inspector.DropTableQuery(viewName)); err != nil {
        return err
    }

    viewSupportVersion, _ := version.NewVersion("5.0")
    if w.targetMysqlVersion.LessThan(viewSupportVersion) {
        log.Warnf("[worker] Target mysql version %v is lower than 5. Views is not supported. Skipping procedure '%s'")
        return nil
    }

    if _, err := w.targetDb.Exec(w.inspector.DropViewQuery(viewName)); err != nil {
        return err
    }

    return nil
}
func (w *worker) exportTable(job *jobExportTable) error {
    batchInsert := MakeBatchInsert(job.rowsPerStmt, job.tableName, job.columnInfo, w.targetDb, w.maxAllowedPacket)

    selectStmt := w.getColumnStmt(job.columnInfo, true)

    // Execute the query
    var whereCond string
    if job.condition != "" {
        whereCond = fmt.Sprintf(" WHERE %s", job.condition)
    }

    query := fmt.Sprintf("SELECT %v FROM `%v`%s", selectStmt, job.tableName, whereCond)
    log.Debugf("[inspector mysql]: Select all with query: [%s]", query)

    rows, err := w.sourceDb.Query(query)
    if err != nil {
        return err
    }

    // Get column names
    columns, err := rows.Columns()
    if err != nil {
        return err
    }

    // Make a slice for the values
    values := make([][]byte, len(columns))

    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }

    // Allocating rowValues
    rowValues := make([]interface{}, len(columns))

    // Fetch rows
    for rows.Next() {
        // get RawBytes from data
        err = rows.Scan(scanArgs...)

        if err != nil {
            return err
        }

        var size int64

        for i, col := range values {
            if col == nil {
                rowValues[i] = nil
            } else {
                colVal := []byte(col)
                size += int64(len(colVal))

                rowValues[i] = colVal
            }
        }

        // insert
        if err := batchInsert.Insert(rowValues, size); err != nil {
            return err
        }
    }

    if err = rows.Err(); err != nil {
        return err
    }

    if err := batchInsert.Flush(); err != nil {
        return err
    }

    if err := batchInsert.Close(); err != nil {
        return err
    }

    return nil
}
func (w *worker)getColumnStmt(columns map[string]*inspector.Column, hexBlob bool) string {
    sortedColumns := inspector.SortColumnsByIndex(columns)

    // make select stmt
    selectParts := make([]string, len(sortedColumns))
    for j, col := range sortedColumns {
        selectParts[j] = fmt.Sprintf("`%v`", col.Name)
    }

    return strings.Join(selectParts, ",")
}