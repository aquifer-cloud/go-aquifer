package aquifer

import (
	"fmt"
	"time"
	"sync"
	"bytes"
	"context"
	"encoding/json"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

type Record struct {
	RelativePath string
	Data map[string]interface{}
	SnapshotVersion int
}

type DataBatch struct {
	*AquiferFile
	logger *zerolog.Logger
	snapshotVersion int
	stateSequence int
	relativePath string
	jsonSchema map[string]interface{}
	maxCount int
	maxByteSize int
	recordsBuffer bytes.Buffer
	count int
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

func (databatch *DataBatch) GetCount() int {
	return databatch.count
}

func (databatch *DataBatch) IsFull() bool {
	return databatch.count >= databatch.maxCount ||
		   databatch.GetSize() >= databatch.maxByteSize
}

func (databatch *DataBatch) AddRecord(record map[string]interface{}, stateSequence int) (err error) {
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
	databatch.stateSequence = stateSequence

	if databatch.recordsBuffer.Len() >= databatch.chunkSize {
		err = databatch.flushRecords()
	}
	return
}

func (databatch *DataBatch) Complete() (err error) {
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
	return databatch.AquiferFile.Cancel()
}

func (databatch *DataBatch) flushRecords() (err error) {
	_, err = databatch.recordsBuffer.WriteTo(databatch)
	return
}

type DataOutputStream struct {
    service *AquiferService
    logger *zerolog.Logger
    ctx context.Context
    source Dict
    accountId uuid.UUID
    entityType string
    entityId uuid.UUID
    metricsSource string
    schemas map[string](map[string]interface{})
    schemasLock sync.Mutex
    messageSequence int
}

func NewDataOutputStream(service *AquiferService,
						 ctx context.Context,
						 source Dict,
	                     accountId uuid.UUID,
	                     entityType string,
    					 entityId uuid.UUID,
    	                 metricsSource string) (*DataOutputStream) {
	outputstream := DataOutputStream{
		service: service,
		logger: log.Ctx(ctx),
		ctx: ctx,
		source: source,
		accountId: accountId,
		entityType: entityType,
		entityId: entityId,
		metricsSource: metricsSource,
		schemas: make(map[string](map[string]interface{})),
		messageSequence: 0,
	}
	return &outputstream
}

func (outputstream *DataOutputStream) HasSchema(relativePath string) (found bool) {
	outputstream.schemasLock.Lock()
	defer outputstream.schemasLock.Unlock()
	_, found = outputstream.schemas[relativePath]
	return
}

func (outputstream *DataOutputStream) SetSchema(relativePath string, schema map[string]interface{}) {
	outputstream.schemasLock.Lock()
	defer outputstream.schemasLock.Unlock()
	outputstream.schemas[relativePath] = schema
}

func (outputstream *DataOutputStream) GetSchema(relativePath string) map[string]interface{} {
	outputstream.schemasLock.Lock()
	defer outputstream.schemasLock.Unlock()
	return outputstream.schemas[relativePath]
}

func (outputstream *DataOutputStream) Worker(ctx context.Context, outputChan <-chan Record) (err error) {
	currentBatches := make(map[string](*DataBatch))

	moreRecords := true
	for moreRecords {
		var record Record
		select {
        case <-ctx.Done():
            for _, dataBatch := range currentBatches {
				dataBatch.Cancel()
			}
			return
        case record, moreRecords = <-outputChan:
        	if ctx.Err() != nil {
                return
            }
            if !moreRecords {
            	continue
            }

        	relativePath := record.RelativePath
			if !outputstream.HasSchema(relativePath) {
				err = fmt.Errorf("Error schema not found: %s", relativePath)
				return
			}

			dataBatch, batchFound := currentBatches[relativePath]
			if !batchFound || dataBatch.snapshotVersion != record.SnapshotVersion {
				if dataBatch != nil && dataBatch.snapshotVersion != record.SnapshotVersion {
					err = dataBatch.Complete()
					if err != nil {
						return
					}
				}

				outputstream.logger.Info().Msgf("New DataBatch: %s", relativePath)

				jsonSchema := outputstream.GetSchema(relativePath)
				dataBatch = NewDataBatch(outputstream.service,
										 outputstream.logger,
										 outputstream.ctx,
										 outputstream.source,
										 outputstream.accountId,
										 outputstream.entityType,
										 outputstream.entityId,
										 relativePath,
										 jsonSchema,
										 record.SnapshotVersion)
				currentBatches[relativePath] = dataBatch
			}

			//outputstream.messageSequence += 1 // TODO: threaded counter
			err = dataBatch.AddRecord(record.Data, outputstream.messageSequence)
			if err != nil {
				return
			}

			if dataBatch.IsFull() {
				outputstream.logger.Info().Msgf("Full DataBatch: %s", relativePath)
				err = dataBatch.Complete()
				if err != nil {
					return
				}
				delete(currentBatches, relativePath)
			}
		}
	}

	for _, dataBatch := range currentBatches {
		err = dataBatch.Complete()
		if err != nil {
			return
		}
	}
	return
}

func (outputstream *DataOutputStream) CompleteSnapshot(relativePath string,
											           snapshotVersion int) (err error) {
	var token string
	token, err = outputstream.service.GetEntityToken(
		outputstream.ctx,
		outputstream.accountId,
		outputstream.entityType,
		outputstream.entityId)
	if err != nil {
		return
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", "snapshot").
			Set("attributes", make(Dict).
				Set("source", outputstream.source).
				SetString("snapshot_type", "data").
				SetString("relative_path", relativePath).
				SetInt("snapshot_version", snapshotVersion)))

	_, err = outputstream.service.Request(
		outputstream.ctx,
		"POST",
		fmt.Sprintf("/accounts/%s/snapshots", outputstream.accountId),
		reqData,
		token)
	return
}
