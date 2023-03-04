package aquifer

import (
	"fmt"
	"sync"
    "sync/atomic"
	"context"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/barkimedes/go-deepcopy"
    "github.com/mitchellh/hashstructure/v2"
    "github.com/elliotchance/orderedmap/v2"
)

const (
	Record int8 = 1
	SnapshotComplete = 2
    StateUpdate = 3
	SchemaUpdate = 4
)

type OutputMessage struct {
	Type int8
	RelativePath string
	Data map[string]interface{}
	SnapshotVersion int
}

type DataOutputStream struct {
    service *AquiferService
    logger *zerolog.Logger
    ctx context.Context
    source Dict
    job JobInterface
    entityType string
    entityId *uuid.UUID
    allowDiscovery bool
    enabledTransform bool
    metricsSource string
    schemas map[string](map[string]interface{})
    schemasLock sync.Mutex
    workerCounter uint64
    messageSequence uint64
    currentBatches sync.Map
    flushedState map[string]interface{}
    states *orderedmap.OrderedMap[uint64, map[string]interface{}]
    nextState map[string]interface{}
    nextStateHash uint64
    stateLock sync.Mutex
}

func NewDataOutputStream(service *AquiferService,
						 ctx context.Context,
						 source Dict,
	                     job JobInterface,
	                     entityType string,
    					 entityId *uuid.UUID,
    	                 metricsSource string,
                         allowDiscovery bool,
                         enabledTransform bool) (*DataOutputStream) {
	outputstream := DataOutputStream{
		service: service,
		logger: log.Ctx(ctx),
		ctx: ctx,
		source: source,
		job: job,
		entityType: entityType,
		entityId: entityId,
        allowDiscovery: allowDiscovery,
        enabledTransform: enabledTransform,
		metricsSource: metricsSource,
		schemas: make(map[string](map[string]interface{})),
        states: orderedmap.NewOrderedMap[uint64, map[string]interface{}](),
	}
	return &outputstream
}

func (outputstream *DataOutputStream) GetState() map[string]interface{} {
    outputstream.stateLock.Lock()
    defer outputstream.stateLock.Unlock()
    return outputstream.flushedState
}

func (outputstream *DataOutputStream) GetNextState() map[string]interface{} {
    outputstream.stateLock.Lock()
    defer outputstream.stateLock.Unlock()
    return outputstream.nextState
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

func (outputstream *DataOutputStream) GetSchema(relativePath string) (map[string]interface{}, bool) {
	outputstream.schemasLock.Lock()
	defer outputstream.schemasLock.Unlock()
	schema, schemaExists := outputstream.schemas[relativePath]
    return schema, schemaExists
}

func (outputstream *DataOutputStream) FetchState() (err error) {
    outputstream.stateLock.Lock()
    defer outputstream.stateLock.Unlock()

    var token string
    token, err = outputstream.service.GetEntityToken(
        outputstream.ctx,
        outputstream.job.GetAccountId(),
        outputstream.entityType,
        outputstream.entityId)
    if err != nil {
        return
    }

    var data Dict
    data, err = outputstream.service.Request(
        outputstream.ctx,
        "GET",
        fmt.Sprintf("/accounts/%s/jobs/%s/state",
            outputstream.job.GetAccountId(),
            outputstream.job.GetId()),
        RequestOptions{
            Token: token,
            Ignore404: true,
        })
    if err != nil {
        return
    }

    if data != nil {
        flushedState := data.Get("data").Get("attributes").Get("state").Map()

        outputstream.flushedState = flushedState

        var nextState interface{}
        nextState, err = deepcopy.Anything(flushedState)
        if err != nil {
            return
        }
        outputstream.nextState = nextState.(map[string]interface{})
    } else {
        outputstream.flushedState = make(map[string]interface{})
        outputstream.nextState = make(map[string]interface{})
    }

    return
}

func (outputstream *DataOutputStream) UpdateStateByRelativePath(relativePath string, value map[string]interface{}, messageSequence uint64) (err error) {
    outputstream.stateLock.Lock()
    defer outputstream.stateLock.Unlock()

    var newStateInterface interface{}
    newStateInterface, err = deepcopy.Anything(outputstream.nextState)
    if err != nil {
        return
    }
    newState := newStateInterface.(map[string]interface{})

    newState[relativePath] = value

    var newStateHash uint64
    newStateHash, err = hashstructure.Hash(newState, hashstructure.FormatV2, nil)
    if err != nil {
        return
    }

    if outputstream.nextStateHash != newStateHash {
        outputstream.states.Set(messageSequence, newState)
        outputstream.nextState = newState
        outputstream.nextStateHash = newStateHash
    }

    return
}

func (outputstream *DataOutputStream) FlushState(force bool) (err error) {
    outputstream.stateLock.Lock()
    defer outputstream.stateLock.Unlock()

    if !force {
        var minBatchSequence uint64 = 0
        batchCount := 0
        outputstream.currentBatches.Range(func(key, value any) bool {
            batchCount += 1
            messageSequence := value.(*DataBatch).GetFirstMessageSequence()
            if minBatchSequence == 0 || minBatchSequence > messageSequence {
                minBatchSequence = messageSequence
            }
            return true
        })
        var maxStateSequenceMatch uint64 = 0
        if batchCount > 0 {
            for _, currStateSequence := range outputstream.states.Keys() {
                if currStateSequence < minBatchSequence && currStateSequence > maxStateSequenceMatch {
                    maxStateSequenceMatch = currStateSequence
                }
            }
        }

        if batchCount == 0 || maxStateSequenceMatch > 0 {
            var newState map[string]interface{}
            if batchCount == 0 {
                newState = outputstream.states.Back().Value
            } else {
                newState, _ = outputstream.states.Get(maxStateSequenceMatch)
            }

            err = outputstream.doFlush(newState)
            if err != nil {
                return
            }
            outputstream.flushedState = newState
            for _, currStateSequence := range outputstream.states.Keys() {
                if currStateSequence <= maxStateSequenceMatch {
                    outputstream.states.Delete(currStateSequence)
                }
            }
        }
    } else {
        err = outputstream.doFlush(outputstream.nextState)
        if err != nil {
            return
        }
        outputstream.flushedState = outputstream.nextState
        outputstream.states = orderedmap.NewOrderedMap[uint64, map[string]interface{}]()
    }
    return
}

func (outputstream *DataOutputStream) Worker(ctx context.Context, outputChan <-chan OutputMessage) (err error) {
	workerBatches := make(map[string](DataBatchInterface))
    workerId := atomic.AddUint64(&outputstream.workerCounter, 1)

	moreMessages := true
	for moreMessages {
		var message OutputMessage
		select {
        case <-ctx.Done():
            for _, dataBatch := range workerBatches {
				dataBatch.Cancel()
			}
			return
        case message, moreMessages = <-outputChan:
        	if ctx.Err() != nil {
                return
            }
            if !moreMessages {
            	continue
            }

            currentMessageSequence := atomic.AddUint64(&outputstream.messageSequence, 1)
            relativePath := message.RelativePath

            if message.Type == Record {
                if !outputstream.allowDiscovery && !outputstream.HasSchema(relativePath) {
                    err = fmt.Errorf("Error schema not found: %s", relativePath)
                    return
                }

    			dataBatch, batchFound := workerBatches[relativePath]
    			if !batchFound || dataBatch.GetSnapshotVersion() != message.SnapshotVersion {
    				if dataBatch != nil && dataBatch.GetSnapshotVersion() != message.SnapshotVersion {
    					err = dataBatch.Complete()
    					if err != nil {
    						return
    					}
    				}

    				outputstream.logger.Info().Msgf("New DataBatch: %s", relativePath)

    				jsonSchema, schemaExists := outputstream.GetSchema(relativePath)
    				dataBatch = NewDataBatch(outputstream.service,
    										 outputstream.logger,
    										 outputstream.ctx,
    										 outputstream.source,
    										 outputstream.job.GetAccountId(),
    										 outputstream.entityType,
    										 outputstream.entityId,
    										 relativePath,
                                             message.SnapshotVersion,
    										 jsonSchema,
    										 schemaExists,
                                             outputstream.allowDiscovery,
                                             outputstream.enabledTransform)
    				workerBatches[relativePath] = dataBatch
                    outputstream.currentBatches.Store(
                        fmt.Sprintf("%d%s", workerId, relativePath),
                        dataBatch)
    			}

    			err = dataBatch.AddRecord(message.Data, currentMessageSequence)
    			if err != nil {
    				return
    			}

    			if dataBatch.IsFull() {
    				outputstream.logger.Info().Msgf("Full DataBatch: %s", relativePath)
    				err = dataBatch.Complete()
    				if err != nil {
    					return
    				}
                    if !outputstream.HasSchema(relativePath) && dataBatch.GetSchemaExists() {
                        outputstream.SetSchema(relativePath, dataBatch.GetJsonSchema())
                    }
    				delete(workerBatches, relativePath)
                    outputstream.currentBatches.Delete(fmt.Sprintf("%d%s", workerId, relativePath))
    			}
            } else if message.Type == StateUpdate {
                err = outputstream.UpdateStateByRelativePath(message.RelativePath,
                                                             message.Data,
                                                             currentMessageSequence)
                if err != nil {
                    return
                }
                err = outputstream.FlushState(false)
                if err != nil {
                    return
                }
            } else if message.Type == SchemaUpdate {
                // TODO: flush existing batch
                // TODO: update existing batch schemas?
                outputstream.SetSchema(message.RelativePath, message.Data)
            } else if message.Type == SnapshotComplete {
                dataBatch, batchFound := workerBatches[relativePath]
                if batchFound {
                    outputstream.logger.Info().Msgf("Complete DataBatch: %s", relativePath)
                    err = dataBatch.Complete()
                    if err != nil {
                        return
                    }
                    delete(workerBatches, relativePath)
                    outputstream.currentBatches.Delete(fmt.Sprintf("%d%s", workerId, relativePath))
                }

                err = outputstream.CompleteSnapshot(message.RelativePath, message.SnapshotVersion)
                if err != nil {
                    return
                }
            } else {
                err = fmt.Errorf("Unknown OutputMessage.Type: %d", message.Type)
                return
            }
		}
	}

	for _, dataBatch := range workerBatches {
		err = dataBatch.Complete()
		if err != nil {
			return
		}
        outputstream.currentBatches.Delete(fmt.Sprintf(
            "%d%s", workerId, dataBatch.GetRelativePath()))
	}
	return
}

func (outputstream *DataOutputStream) CompleteSnapshot(relativePath string,
											           snapshotVersion int) (err error) {
	var token string
	token, err = outputstream.service.GetEntityToken(
		outputstream.ctx,
		outputstream.job.GetAccountId(),
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
		fmt.Sprintf("/accounts/%s/snapshots", outputstream.job.GetAccountId()),
		RequestOptions{
            Token: token,
            Body: reqData,
        })
	return
}

func (outputstream *DataOutputStream) doFlush(state map[string]interface{}) (err error) {
    var token string
    token, err = outputstream.service.GetEntityToken(
        outputstream.ctx,
        outputstream.job.GetAccountId(),
        outputstream.entityType,
        outputstream.entityId)
    if err != nil {
        return
    }

    reqData := make(Dict).
        Set("data", make(Dict).
            SetString("type", "state").
            Set("attributes", make(Dict).
                Set("state", state)))

    _, err = outputstream.service.Request(
        outputstream.ctx,
        "PUT",
        fmt.Sprintf("/accounts/%s/jobs/%s/state",
            outputstream.job.GetAccountId(),
            outputstream.job.GetId()),
        RequestOptions{
            Token: token,
            Body: reqData,
        })
    return
}
