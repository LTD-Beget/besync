package inspector

import (
    "database/sql"
    log "github.com/Sirupsen/logrus"
    "github.com/hashicorp/go-version"
    "fmt"
    "strings"
    "strconv"
    "bytes"
)

type mysqlInspector struct {
    db *sql.DB
    version *version.Version
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
func (i *mysqlInspector)Views(dbName string) ([]string, error) {
    query := `
        SELECT
            TABLE_NAME as tbl_name
        FROM
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_TYPE='VIEW'
            AND TABLE_SCHEMA=?
    `
    rows, err := i.db.Query(query, dbName)

    if err != nil {
        return nil, err
    }

    var views []string

    for rows.Next() {
        var tableName string
        err := rows.Scan(&tableName)

        if err != nil {
            return nil, err
        }

        log.Debugf("FOUND VIEW: %v", tableName)
        views = append(views, tableName)
    }

    return views, nil
}
func (i *mysqlInspector)Triggers(dbName string) ([]string, error) {
    query := "SHOW TRIGGERS FROM " + dbName
    rows, err := i.db.Query(query)

    if err != nil {
        return nil, err
    }

    var triggers []string

    for rows.Next() {
        var triggerName, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10 []byte
        err := rows.Scan(&triggerName, &_1, &_2, &_3, &_4, &_5, &_6, &_7, &_8, &_9, &_10)

        if err != nil {
            return nil, err
        }

        triggerNameStr := string(triggerName)

        log.Debugf("FOUND TRIGGER: %v", triggerNameStr)
        triggers = append(triggers, triggerNameStr)
    }

    return triggers, nil
}
func (i *mysqlInspector)Procedures(dbName string) ([]string, error) {
    query := `
        SELECT
            SPECIFIC_NAME AS procedure_name
        FROM
            INFORMATION_SCHEMA.ROUTINES
        WHERE
            ROUTINE_TYPE='PROCEDURE'
            AND ROUTINE_SCHEMA=?
    `

    rows, err := i.db.Query(query, dbName)
    if err != nil {
        return nil, err
    }

    var procedures []string

    for rows.Next() {
        var routineName string
        err := rows.Scan(&routineName)

        if err != nil {
            return nil, err
        }

        log.Debugf("Found procedure: %v", routineName)
        procedures = append(procedures, routineName)
    }

    return procedures, nil
}
func (i *mysqlInspector)ShowCreateTable(tableName string) (string, error) {
    query := fmt.Sprintf("SHOW CREATE TABLE `%v`", tableName)
    row := i.db.QueryRow(query)

    var _1, createTable string
    err := row.Scan(&_1, &createTable)

    if err != nil {
        return "", err
    }

    return createTable, nil
}
func (i *mysqlInspector)ShowCreateTrigger(triggerName string) (string, error) {
    query := fmt.Sprintf("SHOW CREATE TRIGGER `%v`", triggerName)
    row := i.db.QueryRow(query)

    var _1, _2, createTriggerSql, _4, _5, _6, _7 string

    // from 5.7.2 we'r got 7 columns in response
    moreColVersion, err := version.NewVersion("5.7.2")
    if err != nil {
        return "", err
    }

    if i.version.LessThan(moreColVersion) {
        err = row.Scan(&_1, &_2, &createTriggerSql, &_4, &_5, &_6)
    } else {
        err = row.Scan(&_1, &_2, &createTriggerSql, &_4, &_5, &_6, &_7)
    }

    if err != nil {
        return "", err
    }

    createTriggerSql = strings.Replace(createTriggerSql, "CREATE DEFINER", "/*!50003 CREATE*/ /*!50017 DEFINER", -1)
    createTriggerSql = strings.Replace(createTriggerSql, " TRIGGER", "*/ /*!50003 TRIGGER", -1)

    createTriggerSql = fmt.Sprintf("%v*/", createTriggerSql)

    return createTriggerSql, nil
}
func (i *mysqlInspector)DropTriggerQuery(triggerName string) string {
    return fmt.Sprintf("DROP TRIGGER IF EXISTS `%v`", triggerName)
}
func (i *mysqlInspector)ShowCreateView(viewName string) (string, error) {
    query := fmt.Sprintf("SHOW CREATE VIEW `%s`", viewName)
    row := i.db.QueryRow(query)

    var _1, createViewSql, _3, _4 string

    err := row.Scan(&_1, &createViewSql, &_3, &_4)
    if err != nil {
        return "", err
    }

    createViewSql = strings.Replace(createViewSql, "CREATE ALGORITHM", "/*!50001 CREATE ALGORITHM", -1)
    createViewSql = strings.Replace(createViewSql, " DEFINER=", " */\n/*!50013 DEFINER=", -1)
    createViewSql = strings.Replace(createViewSql, " VIEW ", " */\n/*!50001 VIEW ", -1)

    createViewSql = fmt.Sprintf("%s */;", createViewSql)

    return createViewSql, nil
}
func (i *mysqlInspector)ColumnTypes(tableName string) (map[string]*Column, error) {
    query := fmt.Sprintf("SHOW COLUMNS FROM `%v`", tableName)

    rows, err := i.db.Query(query)

    if err != nil {
        return nil, err
    }

    columnTypes := make(map[string]*Column)
    rowIndex := 0
    for rows.Next() {
        var field, colType, isNull, key, extra string
        var defaultValue sql.NullString

        err := rows.Scan(&field, &colType, &isNull, &key, &defaultValue, &extra)

        if err != nil {
            panic(err)
            return nil, err
        }

        col := i.parseColumnType(field, colType, isNull, key, extra, defaultValue, rowIndex)
        columnTypes[field] = col
        rowIndex += 1
    }

    return columnTypes, nil
}
func (i *mysqlInspector)parseColumnType(field, colType, isNull, key, extra string, defaultValue sql.NullString, index int) *Column {
    col := &Column{
        Name: field,
        Index: index,
        SqlType: colType,
    }

    colTypeParts := strings.Split(colType, " ")

    if braceIdx := strings.Index(colTypeParts[0], "("); braceIdx > 0 {
        col.ColType = colTypeParts[0][0:braceIdx]

        length, err := strconv.Atoi(strings.Replace(colTypeParts[0][braceIdx + 1:], ")", "", -1))
        if err != nil {
            length = 0
        }

        col.Length = length

        col.Attributes = ""
        if len(colTypeParts) > 1 {
            col.Attributes = colTypeParts[1]
        }
    } else {
        col.ColType = colTypeParts[0]
    }

    if _, isNumeric := numericalTypes[col.ColType]; isNumeric {
        col.IsNumeric = true
    }

    if _, isBlob := blobTypes[col.ColType]; isBlob {
        col.isBlob = true
    }

    return col
}
func (i *mysqlInspector)DropTableQuery(tableName string) string {
    return fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", tableName)
}
func (i *mysqlInspector)DropViewQuery(viewName string) string {
    return fmt.Sprintf("DROP VIEW IF EXISTS `%s`", viewName)
}
func (i *mysqlInspector)DropProcedureQuery(procName string) string {
    return fmt.Sprintf("DROP PROCEDURE IF EXISTS `%s`", procName)
}
func (i *mysqlInspector)MakeCreateTableQuery(tableName string, columnInfo map[string]*Column) string {
    var sqlBuffer bytes.Buffer

    sqlBuffer.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%v` (\n", tableName))

    sortedColumns := SortColumnsByIndex(columnInfo)
    colSql := make([]string, len(sortedColumns))

    for i, colInfo := range sortedColumns {
        colSql[i] = fmt.Sprintf("`%s` %s", colInfo.Name, colInfo.SqlType)
    }

    sqlBuffer.WriteString(strings.Join(colSql, ",\n"))
    sqlBuffer.WriteString("\n);\n")

    return sqlBuffer.String()
}
func (i *mysqlInspector)ShowCreateProcedure(procName string) (string, error) {
    query := fmt.Sprintf("SHOW CREATE PROCEDURE `%s`", procName)
    row := i.db.QueryRow(query)

    var _1, _2, createProcedureSql, _4, _5, _6 string

    err := row.Scan(&_1, &_2, &createProcedureSql, &_4, &_5, &_6)
    if err != nil {
        return "", err
    }

    return createProcedureSql, nil
}
// Тут весьма унылый код (спасибо go-database-sql!), который ищет в таблице колонку с наилучшим индексом
func (i *mysqlInspector)FindPrimaryColumn(tableName string, useAnyIndex bool) (string, error) {
    query := fmt.Sprintf("SHOW INDEX FROM `%s`", tableName)

    dataSet, _, err := i.querySimple(query)
    if err != nil {
        return "", err
    }

    // Pick first column in PK, cardinality doesn't matter
    for _, row := range dataSet {
        if row[2] == "PRIMARY" && row[3] == "1" {
            return row[4], nil
        }
    }

    // If no PK found, try using first UNIQUE index
    for _, row := range dataSet {
        if row[1] == "0" && row[3] == "1" {
            return row[4], nil
        }
    }

    if (!useAnyIndex) {
        return "", nil
    }

    // Still unlucky? Pick any high-cardinality index
    var field string

    var max_cardinality int64
    var cardinality int64

    for _, row := range dataSet {
        if row[3] != "1" {
            continue
        }

        if row[6] != "" {
            cardinality, err = strconv.ParseInt(row[6], 10, 64)
            if err != nil {
                continue
            }
        }

        if cardinality > max_cardinality {
            field = row[4]
            max_cardinality = cardinality
        }
    }

    return field, nil
}
func (i *mysqlInspector)GetMinMaxValues(tableName, column string) (min, max string, err error) {
    query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ IFNULL(MIN(`%s`), 0), IFNULL(MAX(`%s`), 0) FROM `%s`", column, column, tableName)

    row := i.db.QueryRow(query)
    err = row.Scan(&min, &max)

    if err != nil {
        return "", "", err
    }

    return
}
func (i *mysqlInspector)EstimateCount(tableName, column string) (int64, error) {
    if column == "" {
        column = "*"
    }

    query := fmt.Sprintf("EXPLAIN SELECT `%s` FROM `%s`", column, tableName)

    dataSet, columnNames, err := i.querySimple(query)
    if err != nil {
        return 0, err
    }

    index := 0
    for i, name := range columnNames {
        if name == "rows" {
            index = i
            break
        }
    }

    count, err := strconv.ParseInt(dataSet[0][index], 10, 64)
    if err != nil {
        return 0, err
    }

    return count, nil

}

func (i *mysqlInspector)querySimple(query string) ([][]string, []string, error) {
    rows, err := i.db.Query(query)
    if err != nil {
        return nil, nil, err
    }

    columns, err := rows.Columns()
    if err != nil {
        return nil, nil, err
    }

    values := make([][]byte, len(columns))
    scanArgs := make([]interface{}, len(values))
    for i := range values {
        scanArgs[i] = &values[i]
    }

    var dataSet [][]string
    j := 0

    for rows.Next() {
        err = rows.Scan(scanArgs...)
        if err != nil {
            return nil, nil, err
        }

        rowValues := make([]string, len(columns))
        for i, col := range values {
            if col == nil {
                rowValues[i] = ""
            } else {
                rowValues[i] = string(col)
            }
        }

        // Унылое копирование, потому что потом нам надо несколько раз пробежаться по dataSet :(
        dataSet = append(dataSet, make([]string, 0))
        dataSet[j] = append(dataSet[j], rowValues...)
        j += 1
    }

    return dataSet, columns, nil
}

func MakeMysqlInspector(db *sql.DB, version *version.Version) *mysqlInspector {
    return &mysqlInspector{
        db: db,
        version: version,
    }
}

var numericalTypes = map[string]bool{
    "bit": true,
    "tinyint": true,
    "smallint": true,
    "mediumint": true,
    "int": true,
    "integer": true,
    "bigint": true,
    "real": true,
    "double": true,
    "float": true,
    "decimal": true,
    "numeric": true,
}

var blobTypes = map[string]bool{
    "tinyblob": true,
    "blob": true,
    "mediumblob": true,
    "longblob": true,
    "binary": true,
    "varbinary": true,
    "bit": true,
}