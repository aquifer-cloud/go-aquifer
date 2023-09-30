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
    GetId() *uuid.UUID
    GetRelativePath() string
    GetSequence() int
    GetSnapshotVersion() int
    GetIdempotentId() string
    GetHyperbatchId() *uuid.UUID
    GetSchemaExists() bool
    GetJsonSchema() map[string]interface{}
    GetCount() int
    IsFull() bool
    AddRecord(map[string]interface{}, uint64) error
    GetLastRecordAddedAt() time.Time
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
    hyperbatchId *uuid.UUID
    firstMessageSequence uint64
    relativePath string
    jsonSchema map[string]interface{}
    schemaExists bool
    allowDiscovery bool
    enabledTransform bool
    maxCount int
    maxByteSize int
    recordsBuffer bytes.Buffer
    records []map[string]interface{}
    readCurrentLine []byte
    count int
    lastRecordAdded time.Time
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
                  entityId *uuid.UUID,
                  relativePath string,
                  snapshotVersion int,
                  jsonSchema map[string]interface{},
                  schemaExists bool,
                  allowDiscovery bool,
                  enabledTransform bool,
                  maxCount int,
                  maxByteSize int) (*DataBatch) {
    if maxCount == 0 {
        maxCount = 100000
    }
    if maxByteSize == 0 {
        maxByteSize = 1024 * 1024 * 16 // 16 MB
    }

    id := uuid.New()
    dataBatch := &DataBatch{
        AquiferFile: NewAquiferFile(service,
                                    ctx,
                                    "wb",
                                    true,
                                    "data-batch",
                                    accountId,
                                    entityType,
                                    entityId,
                                    &id),
        logger: logger,
        writeMode: true,
        snapshotVersion: snapshotVersion,
        sequence: int(time.Now().UTC().Unix()),
        relativePath: relativePath,
        jsonSchema: jsonSchema,
        maxCount: maxCount,
        maxByteSize: maxByteSize,
        count: 0,
        schemaExists: schemaExists,
        allowDiscovery: allowDiscovery,
        enabledTransform: enabledTransform,
    }

    getAttributes := func (file *AquiferFile) (attributes Dict, err error) {
        attributes = make(Dict).
            Set("source", source).
            SetString("idempotent_id", file.fileId.String()).
            SetInt("sequence", dataBatch.GetSequence()).
            SetString("relative_path", relativePath).
            Set("json_schema", dataBatch.GetJsonSchema()).
            SetInt("count", dataBatch.GetCount())

        if snapshotVersion > 0 {
            attributes = attributes.SetInt("snapshot_version", snapshotVersion)
        }

        return
    }

    dataBatch.SetAttributesFn(getAttributes)

    return dataBatch
}

func (databatch *DataBatch) GetId() *uuid.UUID {
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

func (databatch *DataBatch) GetHyperbatchId() *uuid.UUID {
    return databatch.hyperbatchId
}

func (databatch *DataBatch) GetSchemaExists() bool {
    return databatch.schemaExists
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

func (databatch *DataBatch) GetLastRecordAddedAt() time.Time {
    return databatch.lastRecordAdded
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

    // TODO: threadsafe?
    databatch.count += 1

    if !databatch.schemaExists && databatch.allowDiscovery {
        databatch.records = append(databatch.records, record)
    } else {
        if databatch.schemaExists && databatch.enabledTransform {
            record, err = Transform(databatch.jsonSchema, record)
            if err != nil {
                return
            }
        }

        err = databatch.bufferRecord(record)
        if err != nil {
            return
        }
    }

    // TODO: threadsafe?
    databatch.lastRecordAdded = time.Now()

    // Need thread safety - The DataOutputStream check batch messageSequences
    atomic.CompareAndSwapUint64(&databatch.firstMessageSequence, 0, messageSequence)

    if databatch.recordsBuffer.Len() >= databatch.chunkSize {
        err = databatch.flushRecords()
    }
    return
}

func (databatch *DataBatch) bufferRecord(record map[string]interface{}) (err error) {
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
    return
}

func (databatch *DataBatch) Complete() (err error) {
    if !databatch.writeMode {
        err = fmt.Errorf("DataBatch not in write mode")
        return
    }

    if databatch.count == 0 {
        return
    }

    if !databatch.schemaExists && databatch.allowDiscovery {
        databatch.jsonSchema, err = DiscoverSchema(databatch.records)
        if err != nil {
            return
        }
        databatch.schemaExists = true

        properties := databatch.jsonSchema["properties"].(map[string]interface{})
        properties["__aq_id"] = map[string]interface{}{
            "type": "string",
            "primaryKey": true,
            "format": "uuid",
        }

        for _, record := range databatch.records {
            record, err = Transform(databatch.jsonSchema, record)
            if err != nil {
                return
            }
            err = databatch.bufferRecord(record)
            if err != nil {
                return
            }
        }
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
    if len(databatch.records) > 0 {
        exists = true
        record = databatch.records[0]
        databatch.records = databatch.records[1:]
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
        databatch.records = append(databatch.records, lineRecord)
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
