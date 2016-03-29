package dumper

import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    log "github.com/Sirupsen/logrus"
    "os"
    "github.com/go-sql-driver/mysql"
    "gitlab.beget.ru/golang/beget_mysqldump/inspector"
)

func init() {
    log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

    log.SetLevel(log.DebugLevel)
    log.SetOutput(os.Stdout)
}

type DumpSettings struct {
    DatabaseName string
}

type schema struct {
    tables *[]string
}

type dumper struct {
    settings *DumpSettings
    schema *schema
    inspector inspector.Inspector
    db *sql.DB
}

func MakeDumper(settings *DumpSettings) *dumper {
    return &dumper{
        settings: settings,
        schema:&schema{},
    }
}

func (d *dumper) Dump() {
    // TODO Flow(?):
    // connect
    // write header
    // add create/drop database if needed
    // lock database if needed
    // fetch all tables
    // do tables dump in parallel (with triggers)
    // dump stored procedures, functions and other global things

    mysqlConfig := &mysql.Config{
        User: "root",
        Passwd: "testpw",
        Addr: "localhost:3307",
        Net: "tcp",
        DBName: d.settings.DatabaseName,
    }

    db, err := sql.Open("mysql", mysqlConfig.FormatDSN())

    if err != nil {
        log.Panic(err)
    }

    d.db = db
    d.inspector = inspector.MakeMysqlInspector(db)

    err = d.inspect()

    if err != nil {
        log.Panic(err)
    }
}

func (d *dumper)inspect() error {
    tables, err := d.inspector.Tables(d.settings.DatabaseName)

    if err != nil {
        return err
    }

    d.schema.tables = &tables
    log.Infof("Inspected table names: %+v", d.schema.tables)

    return nil
}