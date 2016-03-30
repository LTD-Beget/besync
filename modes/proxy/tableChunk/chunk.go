package tableChunk

import (
    "strconv"
    "fmt"
    "github.com/LTD-Beget/besync/inspector"
    log "github.com/Sirupsen/logrus"
)

type Chunk struct {
    TableName string
    Condition string
}

func CalculateChunksForTable(tableName string, chunkSize int64, i inspector.Inspector) ([]*Chunk, error){
    chunks := make([]*Chunk, 0)

    // get suitable indexed column
    col, err := i.FindPrimaryColumn(tableName, true)
    if err != nil {
        return nil, err
    }

    // if no suitable index found, return only one chunk
    if col == "" {
        chunks = append(chunks, &Chunk{
            TableName: tableName,
        })
        return chunks, nil
    }

    min, max, err := i.GetMinMaxValues(tableName, col)
    if err != nil {
        return nil, err
    }

    rowCount, err := i.EstimateCount(tableName, col)
    if err != nil {
        return nil, err
    }

    if chunkSize == 0 {
        chunkSize = 350000
    }

    est_chunks := int(rowCount / int64(chunkSize))
    if est_chunks == 0 {
        est_chunks = 1
    }

    // Currently we'r support only integer values
    nmin, err := strconv.ParseInt(min, 10, 64)
    log.Debugf("[chunk] [CHUNK: %v] NMIN IS %v", tableName, nmin)
    if err != nil {
        chunks = append(chunks, &Chunk{
            TableName: tableName,
        })
        return chunks, nil
    }

    nmax, err := strconv.ParseInt(max, 10, 64)
    log.Debugf("[chunk] [CHUNK: %v] NMAX IS %v", tableName, nmax)
    if err != nil {
        chunks = append(chunks, &Chunk{
            TableName: tableName,
        })
        return chunks, nil
    }

    est_step := (nmax - nmin) / int64(est_chunks) + 1

    log.Debugf("[chunk] CHUNKS: %v; est_step: %v", est_chunks, est_step)

    counter := 0
    showed_nulls := false
    format := "%s(`%s` >= %d AND `%s` < %d)"
    for cutoff := nmin; cutoff <= nmax; {
        var _1 string

        if !showed_nulls {
            _1 = fmt.Sprintf("`%s` IS NULL OR ", col)
        }

        chunks = append(chunks, &Chunk{
            TableName: tableName,
            Condition: fmt.Sprintf(format, _1, col, cutoff, col, cutoff + est_step),
        })

        cutoff += est_step
        showed_nulls = true
        counter += 1
    }

    return chunks, err
}