package aquifer

import (
    "io"
    "fmt"
    "time"
    "bytes"
    "context"
    "sync/atomic"
    "encoding/json"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
)

type DataBatchInterface interface {
    GetId() uuid.UUID
    GetRelativePath() string
    GetSequence() int
    GetSnapshotVersion() int
    GetIdempotentId() string
    GetHyperbatchId() uuid.UUID
    GetJsonSchema() map[string]interface{}
    GetCount() int
    IsFull() bool
    AddRecord(map[string]interface{}, uint64) error
    Complete() error
    Cancel() error
    NextRecord() (map[string]interface{}, bool, error)
}

type DataBatch struct {
    *AquiferFile
    logger *zerolog.Logger
    writeMode bool
    snapshotVersion int
    sequence int
    idempotentId string
    hyperbatchId uuid.UUID
    firstMessageSequence uint64
    relativePath string
    jsonSchema map[string]interface{}
    maxCount int
    maxByteSize int
    recordsBuffer bytes.Buffer
    readRecords []map[string]interface{}
    readCurrentLine []byte
    count int
}

func ReadDataBatch(job JobInterface) (*DataBatch) {
    attributes := job.GetJobAttributes()
    snapshotVersion, _ := attributes.GetInt("snapshot_version")
    sequence, _ := attributes.GetInt("sequence")
    count, _ := attributes.GetInt("count")
    useGzip := attributes.GetString("compression") == "gzip"

    dataBatch := &DataBatch{
        AquiferFile: NewAquiferFile(job.GetService(),
                                    job.GetCtx(),
                                    "rb",
                                    useGzip,
                                    "data-batch",
                                    job.GetAccountId(),
                                    job.GetEntityType(),
                                    job.GetEntityId(),
                                    job.GetId()),
        logger: job.Logger(),
        idempotentId: attributes.GetString("idempotent_id"),
        snapshotVersion: snapshotVersion,
        sequence: sequence,
        relativePath: attributes.GetString("relative_path"),
        jsonSchema: attributes.Get("json_schema"),
        count: count,
    }
    return dataBatch
}

func NewDataBatch(service *AquiferService,
                  logger *zerolog.Logger,
                  ctx context.Context,
                  source Dict,
                  accountId uuid.UUID,
                  entityType string,
                  entityId uuid.UUID,
                  relativePath string,
                  jsonSchema map[string]interface{},
                  snapshotVersion int) (*DataBatch) {

    dataBatch := &DataBatch{
        AquiferFile: NewAquiferFile(service,
                                    ctx,
                                    "wb",
                                    true,
                                    "data-batch",
                                    accountId,
                                    entityType,
                                    entityId,
                                    uuid.New()),
        logger: logger,
        writeMode: true,
        snapshotVersion: snapshotVersion,
        relativePath: relativePath,
        jsonSchema: jsonSchema,
        // TODO: make maxCount and maxByteSize settable
        maxCount: 1000000,
        maxByteSize: 1024 * 1024 * 16,
        count: 0,
    }

    getAttributes := func (file *AquiferFile) (attributes Dict, err error) {
        attributes = make(Dict).
            Set("source", source).
            SetString("idempotent_id", file.fileId.String()).
            SetInt("sequence", int(time.Now().Unix())).
            SetString("relative_path", relativePath).
            Set("json_schema", jsonSchema).
            SetInt("count", dataBatch.GetCount())

        if snapshotVersion > 0 {
            attributes = attributes.SetInt("snapshot_version", snapshotVersion)
        }

        return
    }

    dataBatch.SetAttributesFn(getAttributes)

    return dataBatch
}

func (databatch *DataBatch) GetId() uuid.UUID {
    return databatch.fileId
}

func (databatch *DataBatch) GetRelativePath() string {
    return databatch.relativePath
}

func (databatch *DataBatch) GetSequence() int {
    return databatch.sequence
}

func (databatch *DataBatch) GetSnapshotVersion() int {
    return databatch.snapshotVersion
}

func (databatch *DataBatch) GetIdempotentId() string {
    return databatch.idempotentId
}

func (databatch *DataBatch) GetHyperbatchId() uuid.UUID {
    return databatch.hyperbatchId
}

func (databatch *DataBatch) GetJsonSchema() map[string]interface{} {
    return databatch.jsonSchema
}

func (databatch *DataBatch) GetCount() int {
    return databatch.count
}

func (databatch *DataBatch) GetFirstMessageSequence() uint64 {
    return atomic.LoadUint64(&databatch.firstMessageSequence)
}

func (databatch *DataBatch) IsFull() bool {
    return databatch.count >= databatch.maxCount ||
           databatch.GetSize() >= databatch.maxByteSize
}

func (databatch *DataBatch) AddRecord(record map[string]interface{}, messageSequence uint64) (err error) {
    if !databatch.writeMode {
        err = fmt.Errorf("DataBatch not in write mode")
        return
    }

    var recordsBytes []byte
    recordsBytes, err = json.Marshal(record)
    if err != nil {
        return
    }

    _, err = databatch.recordsBuffer.Write(recordsBytes)
    if err != nil {
        return
    }
    _, err = databatch.recordsBuffer.Write([]byte{'\n'})
    if err != nil {
        return
    }

    databatch.count += 1
    // Need thread safety - The DataOutputStream check batch messageSequences
    atomic.CompareAndSwapUint64(&databatch.firstMessageSequence, 0, messageSequence)

    if databatch.recordsBuffer.Len() >= databatch.chunkSize {
        err = databatch.flushRecords()
    }
    return
}

func (databatch *DataBatch) Complete() (err error) {
    if !databatch.writeMode {
        err = fmt.Errorf("DataBatch not in write mode")
        return
    }

    databatch.logger.
        Info().
        Str("batch_id", databatch.GetId().String()).
        Str("relative_path", databatch.GetRelativePath()).
        Int("count", databatch.GetCount()).
        Msg("Completing DataBatch")

    err = databatch.flushRecords()
    if err != nil {
        return
    }
    err = databatch.AquiferFile.Close()
    if err != nil {
        return
    }

    databatch.logger.
        Info().
        Str("batch_id", databatch.GetId().String()).
        Str("relative_path", databatch.GetRelativePath()).
        Int("count", databatch.GetCount()).
        Msg("Completed DataBatch")

    return
}

func (databatch *DataBatch) Cancel() error {
    if !databatch.writeMode {
        return fmt.Errorf("DataBatch not in write mode")
    }

    return databatch.AquiferFile.Cancel()
}

func (databatch *DataBatch) NextRecord() (record map[string]interface{}, exists bool, err error) {
    if len(databatch.readRecords) > 0 {
        exists = true
        record = databatch.readRecords[0]
        databatch.readRecords = databatch.readRecords[1:]
        return
    }

    chunk := make([]byte, databatch.GetChunkSize())
    var n int
    n, err = databatch.Read(chunk)
    if err != nil && err != io.EOF {
        return
    }

    if err == io.EOF {
        err = nil
        return
    }

    lines := bytes.Split(chunk[:n], []byte("\n"))
    for i, line := range lines[:len(lines) - 1] {
        if i == 0 {
            line = append(databatch.readCurrentLine, line...)
            databatch.readCurrentLine = make([]byte, 0)
        }
        var lineRecord map[string]interface{}
        err = Unmarshal(line, &lineRecord)
        if err != nil {
            return
        }
        databatch.readRecords = append(databatch.readRecords, lineRecord)
    }
    databatch.readCurrentLine = append(databatch.readCurrentLine, lines[len(lines) - 1]...)

    return databatch.NextRecord()
}

func (databatch *DataBatch) flushRecords() (err error) {
    if !databatch.writeMode {
        err = fmt.Errorf("DataBatch not in write mode")
        return
    }

    _, err = databatch.recordsBuffer.WriteTo(databatch)
    return
}
