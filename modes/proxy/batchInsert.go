package proxy

import (
    "fmt"
    "github.com/LTD-Beget/besync/inspector"
    "sort"
    "bytes"
    "strings"
    "database/sql"
    "math"
    log "github.com/Sirupsen/logrus"
)

type batchInsert struct {
    cap          int // максимальное количество строк, которые могут быть вставлены за раз
    table        string
    columnInfo   map[string]*inspector.Column
    statement    *sql.Stmt
    statementLen int // количество строк в текущем стейтменте
    dataChunkLen int // количество колонок в строке
    db           *sql.DB
    data         []interface{}
    curDataLen   int
    maxPacketLen int64
    curDataSize  int64
}

const MAX_PLACEHOLDERS = 60000
const MAX_CAP float64 = 5999

func MakeBatchInsert(cap int, table string, columnInfo map[string]*inspector.Column, db *sql.DB, maxPacketLen int64) *batchInsert {
    dataChunkLen := len(columnInfo)

    if cap == 0 || cap * dataChunkLen > MAX_PLACEHOLDERS {
        cap = int(math.Floor(float64(MAX_PLACEHOLDERS) / float64(dataChunkLen))) - 1
        cap = int(math.Min(MAX_CAP, float64(cap)))

        log.Infof("[batch insert] CAP SET TO %v", cap)
    }

    return &batchInsert{
        cap: cap,
        table: table,
        columnInfo: columnInfo,
        db: db,
        data: make([]interface{}, cap * dataChunkLen),
        dataChunkLen: dataChunkLen,
        maxPacketLen: maxPacketLen,
    }
}
func (b *batchInsert)Insert(rowValues []interface{}, size int64) error {
    valuesLen := len(rowValues)

    if valuesLen != b.dataChunkLen {
        return fmt.Errorf("[batch insert] Invalid row values count: %v, needed %v", valuesLen, b.dataChunkLen)
    }

    b.curDataSize += size

    if b.curDataSize >= b.maxPacketLen {
        log.Warnf("[batch insert][%s] Data size(%v) >= maxPacketLen(%v)! Flushing...", b.table, b.curDataSize, b.maxPacketLen)

        if err := b.Flush(); err != nil {
            return err
        }

        // because curDataSize is reset by flush
        b.curDataSize += size
    }

    offset := b.curDataLen * b.dataChunkLen
    for i, value := range rowValues {
        b.data[offset + i] = value
    }

    b.curDataLen += 1

    if b.curDataLen == b.cap {
        return b.Flush()
    }

    return nil
}

func (b *batchInsert)Close() error {
    if b.statement != nil {
        if err := b.statement.Close(); err != nil {
            return err
        }
    }

    return nil
}

func (b *batchInsert)Flush() error {
    if b.curDataLen == 0 {
        return nil
    }

    if b.statementLen != b.curDataLen {
        // close prev statement
        if err := b.Close(); err != nil {
            return err
        }

        // Make new statement
        if stmt, err := b.makeStatement(); err != nil {
            return err
        } else {
            b.statement = stmt
            b.statementLen = b.curDataLen
        }
    }

    params := b.data[0:b.statementLen * b.dataChunkLen]

    if _, err := b.statement.Exec(params...); err != nil {
        return err
    }

    // rewind data index
    b.curDataLen = 0
    b.curDataSize = 0

    return nil
}
func (b *batchInsert)makeStatement() (*sql.Stmt, error) {
    insertQuery, err := b.makeInsertQuery()
    if err != nil {
        return nil, err
    }

    stmt, err := b.db.Prepare(insertQuery)
    if err != nil {
        return nil, err
    }

    return stmt, nil
}
type ByIndex []*inspector.Column

func (a ByIndex) Len() int {
    return len(a)
}
func (a ByIndex) Swap(i, j int) {
    a[i], a[j] = a[j], a[i]
}
func (a ByIndex) Less(i, j int) bool {
    return a[i].Index < a[j].Index
}
func (b *batchInsert)makeInsertQuery() (string, error) {
    if b.columnInfo == nil {
        return "", fmt.Errorf("[batch insert] Cannot make insert query, because column info for table %s is not set", b.table)
    }

    // convert to slice & sort
    sortedColumns := make([]*inspector.Column, len(b.columnInfo))
    j := 0

    for _, col := range b.columnInfo {
        sortedColumns[j] = col
        j += 1
    }

    sort.Sort(ByIndex(sortedColumns))

    // create statement to insert values into table
    insertParts := make([]string, len(sortedColumns))
    for j, col := range sortedColumns {
        insertParts[j] = fmt.Sprintf("`%s`", col.Name)
    }

    // make placeholders string
    plBuf := bytes.Buffer{}
    plBuf.WriteString("?")
    for i := 1; i < len(sortedColumns); i++ {
        // len - 1 iterations
        plBuf.WriteString(",?")
    }
    paramsStr := fmt.Sprintf("(%v)", plBuf.String())

    paramsBuf := bytes.Buffer{}
    paramsBuf.WriteString(paramsStr)
    for i := 0; i < b.curDataLen - 1; i++ {
        paramsBuf.WriteString(fmt.Sprintf(",%v", paramsStr))
    }

    return fmt.Sprintf("INSERT INTO `%v` (%v) VALUES %v", b.table, strings.Join(insertParts, ","), paramsBuf.String()), nil
}