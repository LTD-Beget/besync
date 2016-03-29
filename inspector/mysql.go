package inspector

import (
    "database/sql"
    log "github.com/Sirupsen/logrus"
)

type mysqlInspector struct {
    db *sql.DB
}
func (i *mysqlInspector) Tables(dbName string) ([]string, error) {
    query := `
        SELECT
            TABLE_NAME as tbl_name
        FROM
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_TYPE='BASE TABLE'
            AND TABLE_SCHEMA=?
    `
    rows, err := i.db.Query(query, dbName)

    if err != nil {
        return nil, err
    }

    var tables []string

    for rows.Next() {
        var tableName string
        err := rows.Scan(&tableName)

        if err != nil {
            return nil, err
        }

        log.Debugf("FOUND TABLE: %v", tableName)
        tables = append(tables, tableName)
    }

    return tables, nil
}


func MakeMysqlInspector(db *sql.DB) *mysqlInspector {
    return &mysqlInspector{
        db: db,
    }
}
