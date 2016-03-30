package tableChunk

import (
    "sync"
    "math"
)

type Manager struct {
    chunks                    map[string]*chunkInfo
    mutex                     *sync.Mutex
    maxProcessing             int
    maxProcessingForLastTable int
    currentProcessing         map[string]int // tableName -> countProcessing
    chunkCh                   chan *Chunk
}

func MakeManager(maxProcessing, maxProcessingForLastTable int) *Manager {
    return &Manager{
        chunks: make(map[string]*chunkInfo),
        mutex: &sync.Mutex{},
        maxProcessing: maxProcessing,
        maxProcessingForLastTable: maxProcessingForLastTable,
        chunkCh: make(chan *Chunk, maxProcessing),
        currentProcessing: make(map[string]int),
    }
}

func (m *Manager)AddChunk(chunk *Chunk) {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    chunks, ok := m.chunks[chunk.TableName]
    if !ok {
        chunks = &chunkInfo{
            tableName: chunk.TableName,
            chunks: make([]*Chunk, 0),
        }

        m.chunks[chunk.TableName] = chunks
    }

    chunks.add(chunk)
}
func (m *Manager)GetNext() *Chunk {
    m.recalculateAndSend()
    return <-m.chunkCh
}
func (m *Manager)recalculateAndSend() {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    if m.getProcessingCountAll() >= m.maxProcessing {
        return
    }

    minChunks := m.getChunksForMinScoreTable()

    if minChunks == nil {
        m.chunkCh <- nil
    } else {
        // Если осталась только одна таблица и она сейчас процессится, то
        notProcessedChunks := m.getChunksForNotProcessedTables()

        if len(notProcessedChunks) == 1 && m.getProcessingCountTable(minChunks.tableName) > m.maxProcessingForLastTable - 1 {
            return
        }

        chunk := minChunks.getNext()
        m.currentProcessing[chunk.TableName] += 1
        m.chunkCh <- chunk
    }
}

func (m *Manager)getChunksForMinScoreTable() *chunkInfo {
    var minChunks *chunkInfo
    minScore := math.MaxInt32

    for _, tableChunks := range m.chunks {
        if tableChunks.noChunks() {
            continue
        }

        score := m.getProcessingCountTable(tableChunks.tableName)

        if score < minScore {
            minChunks = tableChunks
            minScore = score
        }
    }

    return minChunks
}

func (m *Manager)getChunksForNotProcessedTables() []*chunkInfo {
    var chunks []*chunkInfo

    for _, tableChunks := range m.chunks {
        if !tableChunks.noChunks() {
            chunks = append(chunks, tableChunks)
        }
    }

    return chunks
}

func (m *Manager) GetProcessing() map[string]int {
    m.mutex.Lock()
    defer m.mutex.Unlock()

    result := make(map[string]int)
    for tableName, countProcessing := range m.currentProcessing {
        if countProcessing > 0 {
            result[tableName] = countProcessing
        }
    }

    return result
}
func (m *Manager) Done(tableName string) {
    m.mutex.Lock()
    m.currentProcessing[tableName] -= 1
    m.mutex.Unlock()

    m.recalculateAndSend()
}
func (m *Manager) getProcessingCountTable(tableName string) int {
    if count, ok := m.currentProcessing[tableName]; ok {
        return count
    } else {
        return 0
    }
}
func (m *Manager) getProcessingCountAll() int {
    var count int

    for _, tableCount := range m.currentProcessing {
        count += tableCount
    }

    return count
}

type chunkInfo struct {
    tableName string
    nextIdx   int
    chunks    []*Chunk
}

func (c *chunkInfo)add(chunk *Chunk) {
    c.chunks = append(c.chunks, chunk)
}
func (c *chunkInfo)noChunks() bool {
    return c.nextIdx == len(c.chunks)
}
func (c *chunkInfo)getNext() *Chunk {
    if c.noChunks() {
        return nil
    }

    chunk := c.chunks[c.nextIdx]
    c.nextIdx += 1

    return chunk
}