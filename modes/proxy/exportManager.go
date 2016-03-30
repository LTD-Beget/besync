package proxy

import (
    log "github.com/Sirupsen/logrus"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "os"
    "time"
    "sync"
    "encoding/json"
    "errors"
)

type exportManager struct {
    db *sql.DB
    mutex *sync.Mutex
}

type ExportStatus struct {
    Id int64
    Status string
    Error error
}

var tableSchema = `
CREATE TABLE IF NOT EXISTS sync_task
(
 id INT UNIQUE NOT NULL,
 status VARCHAR(50) NOT NULL,
 settings TEXT NOT NULL,
 error_text TEXT,
 date_create DATETIME NOT NULL,
 date_update DATETIME NOT NULL
);
`

var Manager exportManager

func init() {
    var dbName string
    var ok bool

    if dbName, ok = os.LookupEnv("MD_LOCAL_DB_NAME"); !ok {
        dbName = "./db.db"
    }

    db, err := sql.Open("sqlite3", dbName)

    if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
        log.Panicf("exportManager init error: %v", err)
    }

    if err != nil {
        log.Panicf("exportManager init error: %v", err)
    }

    if _, err := db.Exec(tableSchema); err != nil {
        log.Panicf("exportManager init error: %v", err)
    }

    Manager = exportManager{
        db: db,
        mutex: &sync.Mutex{},
    }
}

func (m *exportManager)StartDump(settings *Settings, resultCh chan *ExportStatus) (int64, error) {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    id := time.Now().UnixNano()

    exporter := MakeExporter(settings)

    dumpedSettings, err := json.Marshal(exporter.settings)
    if err != nil {
        return 0, err
    }

    go func() {
        defer func() {
            if r := recover(); r != nil {
                switch x := r.(type) {
                case string:
                    err = errors.New(x)
                case error:
                    err = x
                default:
                    err = errors.New("Unknown panic")
                }

                if handleErr := m.handleError(id, err, resultCh); handleErr != nil {
                    panic(handleErr)
                }
            }
        }()

        if err := exporter.Start(); err != nil {
            if handleErr := m.handleError(id, err, resultCh); handleErr != nil {
                panic(handleErr)
            }
        }

        if err := m.handleSuccess(id, resultCh); err != nil {
            panic(err)
        }
    }()

    insertSql := `INSERT INTO sync_task (id, status, settings, date_create, date_update)
     VALUES (?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))`

    if _, err := m.db.Exec(insertSql, id, "started", string(dumpedSettings)); err != nil {
        return 0, err
    }

    resultCh <- &ExportStatus{
        Id: id,
        Status: "started",
    }

    return id, nil
}

func (m *exportManager)handleError(id int64, err error, resultCh chan *ExportStatus) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    updateSql := "UPDATE sync_task SET status = 'error', error_text = ?, date_update = datetime('now','localtime') WHERE id = ?"

    if _, err := m.db.Exec(updateSql, err.Error(), id); err != nil {
        return err
    }

    resultCh <- &ExportStatus{
        Id: id,
        Status: "error",
        Error: err,
    }

    return nil
}

func (m *exportManager)handleSuccess(id int64, resultCh chan *ExportStatus) error {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    updateSql := "UPDATE sync_task SET status = 'success', date_update = datetime('now','localtime') WHERE id = ?"

    if _, err := m.db.Exec(updateSql, id); err != nil {
        return err
    }

    resultCh <- &ExportStatus{
        Id: id,
        Status: "success",
    }

    return nil
}

func (m *exportManager)GetStatus(id int64) (*ExportStatus, error) {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    var status string
    var error_byte []byte

    rows, err := m.db.Query("SELECT status, error_text FROM sync_task WHERE id = ?", id)
    if err != nil {
        return nil, err
    }

    for rows.Next() {
        if err := rows.Scan(&status, &error_byte); err != nil {
            return nil, err
        }

        var err error
        error_text := string(error_byte)

        if error_text != "" {
            err = errors.New(error_text)
        }

        return &ExportStatus{
            Id: id,
            Status: status,
            Error: err,
        }, nil
    }

    return nil, nil
}