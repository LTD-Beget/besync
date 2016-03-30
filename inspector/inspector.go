package inspector

import "sort"

type Inspector interface {
    Tables(dbName string) ([]string, error)
    Views(dbName string) ([]string, error)
    Triggers(dbName string) ([]string, error)
    Procedures(dbName string) ([]string, error)
    ShowCreateTable(tableName string) (string, error)
    MakeCreateTableQuery(tableName string, columnInfo map[string]*Column) string
    DropTableQuery(tableName string) string

    ShowCreateTrigger(triggerName string) (string, error)
    DropTriggerQuery(triggerName string) string

    ShowCreateView(viewName string) (string, error)
    DropViewQuery(viewName string) string

    ShowCreateProcedure(procName string) (string, error)
    DropProcedureQuery(procName string) string

    ColumnTypes(tableName string) (map[string]*Column, error)

    FindPrimaryColumn(tableName string, useAnyIndex bool) (string, error)
    GetMinMaxValues(tableName, column string) (min, max string, err error)
    EstimateCount(tableName, column string) (int64, error)
}

type RowCallback func(tableName string, rowValues []interface{}) error

type Column struct {
    Name       string
    Index      int // not mysql index, just index in columns set e.g | id(idx=0) | name(idx=1) | ...(idx=n) | ...(idx=n+1) |
    IsNumeric  bool
    isBlob     bool
    ColType    string
    SqlType    string
    Length     int
    Attributes string
}

type ByIndex []*Column
func (a ByIndex) Len() int           { return len(a) }
func (a ByIndex) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByIndex) Less(i, j int) bool { return a[i].Index < a[j].Index
}
func SortColumnsByIndex(columns map[string]*Column) []*Column {
    // convert to slice & sort
    sortedColumns := make([]*Column, len(columns))
    j := 0

    for _, col := range columns {
        sortedColumns[j] = col
        j += 1
    }

    sort.Sort(ByIndex(sortedColumns))

    return sortedColumns
}