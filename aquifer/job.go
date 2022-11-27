package aquifer

import (
	"io"
	"fmt"
	"time"
	"sync"
	"context"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

type EventSource struct {
	Type string
	Id uuid.UUID
	FlowId uuid.UUID
	JobType string
	JobId uuid.UUID
}

type EventDestination struct {
	Type string
	Id uuid.UUID
	FlowId uuid.UUID
	JobType string
	JobId uuid.UUID
	Handle string
}

type AquiferEvent struct {
	Id uuid.UUID
	Type string
	AccountId uuid.UUID
	Timestamp time.Time
	Source EventSource
	Destination EventDestination
	Payload Dict
}

type JobInterface interface {
	Logger() *zerolog.Logger
	GetCtx() context.Context
	GetService() *AquiferService
	IsTimedout() bool
	GetAccountId() uuid.UUID
	GetType() string
	GetId() uuid.UUID
	GetFlowId() uuid.UUID
	GetEntityTypeName() string
	GetEntityType() string
	GetEntityId() uuid.UUID
	GetConfig() Dict
    GetJobAttributes() Dict
    GetDataBatch() DataBatchInterface
    GetDataOutputStream() *DataOutputStream
	Lock() error
	Release(releaseStatus string, failureErrorId uuid.UUID) error
	Touch() error
    GetExtracts() ([]*Extract, error)
}

type FileJobInterface interface {
	JobInterface
	GetFile() io.Reader
}

type FileJob struct {
	*AquiferJob
	File *AquiferFile
}

func (fileJob *FileJob) GetFile() AquiferFileInterface {
	return fileJob.File
}

func (fileJob *FileJob) GetSourcePath() string {
	return fileJob.jobAttributes.GetString("source_path")
}

func (fileJob *FileJob) GetDateModified() string {
	return fileJob.jobAttributes.GetString("date_modified")
}

func (fileJob *FileJob) GetMimeType() string {
	return fileJob.jobAttributes.GetString("mime_type")
}

func (fileJob *FileJob) GetExt() string {
	return fileJob.jobAttributes.GetString("extension")
}

func (fileJob *FileJob) GetCustomMetadata() map[string]interface{} {
	return fileJob.jobAttributes.Get("custom_metadata")
}

func (fileJob *FileJob) GetCustomMetadataSchema() map[string]interface{} {
	return fileJob.jobAttributes.Get("custom_metadata_schema")
}

type AquiferJob struct {
	service *AquiferService
	ctx context.Context
	logger *zerolog.Logger
    dataBatch DataBatchInterface
    dataBatchLock sync.Mutex
	dataOutputStream *DataOutputStream
    dataOutputStreamLock sync.Mutex
	cancel func()
	cancelTimer *time.Timer
	timedout bool
	event AquiferEvent
	accountId uuid.UUID
	entityType string
	entityId uuid.UUID
	flowId uuid.UUID
	jobType string
	jobId uuid.UUID
    relativePath string
	jobAttributes Dict
	entityAttributes Dict
	lockId uuid.UUID
	locked bool
	ackImmediately bool
	lastTouch time.Time
	touchLock sync.Mutex
}

func NewAquiferJobFromEvent(service *AquiferService, ctx context.Context, event AquiferEvent) (job JobInterface, err error) {
	if event.Destination.JobType == "file" || event.Destination.JobType == "data-batch" {
		job = NewFileJobFromEvent(service, ctx, event)
	} else if event.Destination.JobType == "extract" {
		job = NewExtractJobFromEvent(service, ctx, event)
	} else {
		err = fmt.Errorf("Unknown job type: %s", event.Destination.JobType)
	}
	return
}

func NewFileJobFromEvent(service *AquiferService, ctx context.Context, event AquiferEvent) JobInterface {
	jobCtx, jobCancel := context.WithCancel(ctx)

	jobCtx = log.With().
		Str("account_id", event.AccountId.String()).
		Str("job_type", event.Destination.JobType).
		Str("job_id", event.Destination.JobId.String()).
		Str("flow_id", event.Destination.FlowId.String()).
		Str("entity_type", event.Destination.Type).
		Str("entity_id", event.Destination.Id.String()).
		Logger().
		WithContext(jobCtx)
	logger := log.Ctx(jobCtx)

	job := FileJob{
		AquiferJob: &AquiferJob{
			service: service,
			ctx: jobCtx,
			logger: logger,
			cancel: jobCancel,
			event: event,
			accountId: event.AccountId,
			entityType: event.Destination.Type,
			entityId: event.Destination.Id,
			flowId: event.Destination.FlowId,
			jobType: event.Destination.JobType,
			jobId: event.Destination.JobId,
			ackImmediately: false,
		},
		File: NewAquiferFile(service,
							 jobCtx,
					         "rb",
					         false, // On read, this comes from the API response
					         event.Destination.JobType,
					         event.AccountId,
					         event.Destination.Type,
					         event.Destination.Id,
					         event.Destination.JobId),
	}
	return &job
}

func NewExtractJobFromEvent(service *AquiferService, ctx context.Context, event AquiferEvent) JobInterface {
	jobCtx, jobCancel := context.WithCancel(ctx)

	jobCtx = log.With().
		Str("account_id", event.AccountId.String()).
		Str("job_type", event.Destination.JobType).
		Str("job_id", event.Destination.JobId.String()).
		Str("flow_id", event.Destination.FlowId.String()).
		Str("entity_type", event.Destination.Type).
		Str("entity_id", event.Destination.Id.String()).
		Logger().
		WithContext(jobCtx)
	logger := log.Ctx(jobCtx)

	return &AquiferJob{
		service: service,
		ctx: jobCtx,
		logger: logger,
		cancel: jobCancel,
		event: event,
		accountId: event.AccountId,
		entityType: event.Destination.Type,
		entityId: event.Destination.Id,
		flowId: event.Destination.FlowId,
		jobType: event.Destination.JobType,
		jobId: event.Destination.JobId,
		ackImmediately: true,
	}
}

func (job *AquiferJob) GetCtx() context.Context {
	return job.ctx
}

func (job *AquiferJob) GetService() *AquiferService {
	return job.service
}

func (job *AquiferJob) Logger() *zerolog.Logger {
	return job.logger
}

func (job *AquiferJob) GetType() string {
	return job.jobType
}

func (job *AquiferJob) GetId() uuid.UUID {
	return job.jobId
}

func (job *AquiferJob) GetFlowId() uuid.UUID {
	return job.flowId
}

func (job *AquiferJob) GetEntityTypeName() string {
	return job.entityAttributes.GetString("type_name")
}

func (job *AquiferJob) GetEntityType() string {
	return job.entityType
}

func (job *AquiferJob) GetEntityId() uuid.UUID {
	return job.entityId
}

func (job *AquiferJob) GetAccountId() uuid.UUID {
	return job.accountId
}

func (fileJob *FileJob) GetRelativePath() string {
    return fileJob.jobAttributes.GetString("relative_path")
}

func (job *AquiferJob) IsTimedout() bool {
	return job.timedout
}

func (job *AquiferJob) GetTimeout() time.Duration {
	timeout, exists := job.jobAttributes.GetInt("timeout")
	if !exists {
		timeout = 900
	}
	return time.Duration(timeout) * time.Second
}

func (job *AquiferJob) GetConfig() Dict {
	return job.entityAttributes.Get("config")
}

func (job *AquiferJob) GetSchemas() Dict {
	return job.entityAttributes.Get("schemas")
}

func (job *AquiferJob) GetJobAttributes() Dict {
	return job.jobAttributes
}

func (job *AquiferJob) GetDataBatch() DataBatchInterface {
    job.dataBatchLock.Lock()
    defer job.dataBatchLock.Unlock()

    if job.dataBatch == nil {
        job.dataBatch = ReadDataBatch(job)
    }

    return job.dataBatch
}

func (job *AquiferJob) GetDataOutputStream() *DataOutputStream {
    job.dataOutputStreamLock.Lock()
    defer job.dataOutputStreamLock.Unlock()

    if job.dataOutputStream == nil {
        source := make(Dict).
            SetString("type", job.event.Destination.Type).
            SetString("id", job.event.Destination.Id.String()).
            SetString("flow_id", job.event.Destination.FlowId.String()).
            SetString("job_type", job.event.Destination.JobType).
            SetString("job_id", job.event.Destination.JobId.String())

        metricsSource := "load"
        if job.jobType == "extract" {
            metricsSource = "extract"
        } else if job.entityType == "processor" {
            metricsSource = "process"
        }

        job.dataOutputStream = NewDataOutputStream(
            job.service,
            job.GetCtx(),
            source,
            job,
            job.event.Destination.Type,
            job.event.Destination.Id,
            metricsSource)
    }

    return job.dataOutputStream
}

func (job *AquiferJob) Lock() (err error) {
	var token string
	token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
	if err != nil {
		return
	}

	attributes := make(Dict).SetString("worker_id", job.service.workerId)

	if job.event.Destination.Handle != "" {
		attributes = attributes.SetString("handle", job.event.Destination.Handle)
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", fmt.Sprintf("%s-lock", job.getLockPrefix())).
			Set("attributes", attributes))

	var data Dict
	data, err = job.service.Request(
		job.ctx,
		"POST",
		fmt.Sprintf("%s/lock", job.getJobPath()),
		reqData,
		token)
	if err != nil {
		return
	}

	lockId := data.Get("data").Get("attributes").GetString("worker_lock_id")
	job.lockId, err = uuid.Parse(lockId)
	if err != nil {
		return
	}
	job.locked = true
	job.lastTouch = time.Now()

	if job.jobType != "extract" {
		var jobData Dict
		jobData, err = job.service.Request(
			job.ctx,
			"GET",
			job.getJobPath(),
			nil,
			token)
		if err != nil {
			return
		}

		job.jobAttributes = jobData.Get("data").Get("attributes")
	}

	job.cancelTimer = time.AfterFunc(
		job.GetTimeout(),
		func() {
			job.Logger().Error().Msg("Job timeout")
			job.timedout = true
			job.cancel()
		})

	var entityPath string
	entityPath, err = job.service.GetEntityPath(
		job.accountId,
		job.entityType,
		job.entityId)
	if err != nil {
		return
	}

	var entityData Dict
	entityData, err = job.service.Request(
		job.ctx,
		"GET",
		entityPath,
		nil,
		token)
	if err != nil {
		return
	}

	job.entityAttributes = entityData.Get("data").Get("attributes")

	return
}

func (job *AquiferJob) Release(releaseStatus string, failureErrorId uuid.UUID) (err error) {
	defer func() {
		if job.cancelTimer != nil {
			job.cancelTimer.Stop() // TODO: needed?
		}
		job.cancel()
	}()

	if job.locked {
		ctx := job.ctx
		if ctx.Err() != nil {
			var cancel func()
			ctx, cancel = context.WithCancel(context.TODO())
			defer cancel()
		}

		var token string
		token, err = job.service.GetEntityToken(ctx, job.accountId, job.entityType, job.entityId)
		if err != nil {
			return
		}

		attributes := make(Dict)

		if releaseStatus != "" {
			attributes = attributes.SetString("release_status", releaseStatus)
		}

		if failureErrorId != uuid.Nil {
			attributes = attributes.SetString("failure_error_id", failureErrorId.String())
		}

		if job.event.Destination.Handle != "" {
			attributes = attributes.SetString("handle", job.event.Destination.Handle)
		}

		reqData := make(Dict).
			Set("data", make(Dict).
				SetString("type", fmt.Sprintf("%s-release", job.getLockPrefix())).
				Set("attributes", attributes))

		_, err = job.service.Request(
			ctx,
			"POST",
			fmt.Sprintf("%s/locks/%s/release", job.getJobPath(), job.lockId.String()),
			reqData,
			token)
		if err != nil {
			return
		}
		job.locked = false
	}
	return
}

func (job *AquiferJob) Touch() (err error) {
	job.touchLock.Lock()
	defer job.touchLock.Unlock()

	if job.locked && time.Now().Sub(job.lastTouch).Seconds() >= 60.0 {
		var token string
		token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
		if err != nil {
			return
		}

		attributes := make(Dict)

		if job.event.Destination.Handle != "" {
			attributes = attributes.SetString("handle", job.event.Destination.Handle)
		}

		reqData := make(Dict).
			Set("data", make(Dict).
				SetString("type", fmt.Sprintf("%s-touch", job.getLockPrefix())).
				Set("attributes", attributes))

		_, err = job.service.Request(
			job.ctx,
			"POST",
			fmt.Sprintf("%s/locks/%s/touch", job.getJobPath(), job.lockId.String()),
			reqData,
			token)
		if err != nil {
			return
		}
		job.lastTouch = time.Now()
		job.cancelTimer.Reset(job.GetTimeout())
	}
	return
}

type Extract struct {
    Path string
    RelativePath string
    ExtractType string
    OffsetField string
    OffsetInterval float64
    TargetJsonSchema map[string]interface{}
    HashSalt string
    HashAlgo string
}

func (job *AquiferJob) GetExtracts() (extracts []*Extract, err error) {
    var token string
    token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
    if err != nil {
        return
    }

    var data Dict
    data, err = job.service.Request(
        job.ctx,
        "GET",
        fmt.Sprintf("%s/extracts?include=stream,schema", job.getJobPath()),
        nil,
        token)
    if err != nil {
        return
    }

    includes := make(map[string]map[string]interface{})
    for _, includeItem := range data.GetArray("included") {
        includeItemMap := includeItem.(map[string]interface{})
        itemType := includeItemMap["type"].(string)
        if _, exists := includes[itemType]; !exists {
            includes[itemType] = make(map[string]interface{})
        }
        itemId := includeItemMap["id"].(string)
        includes[itemType][itemId] = includeItemMap
    }

    rawExtracts := data.GetArray("data")
    extracts = make([]*Extract, len(rawExtracts))
    for i, rawExtract := range rawExtracts {
        rawExtractDict := rawExtract.(Dict)
        rawExtractAttributes := rawExtractDict.Get("attributes")
        offsetInterval, _ := rawExtractAttributes.GetFloat64("offset_interval")

        streamId := rawExtractDict.
            Get("relationships").
            Get("stream").
            Get("data").
            GetString("id")
        stream := includes["stream"][streamId].(Dict)
        schemaId := stream.
            Get("relationships").
            Get("schema").
            Get("data").
            GetString("id")
        schema := includes["schema"][schemaId].(Dict)

        extracts[i] = &Extract{
            Path: schema.GetString("path"),
            RelativePath: schema.GetString("relative_path"),
            ExtractType: rawExtractAttributes.GetString("extract_type"),
            OffsetField: rawExtractAttributes.GetString("offset_field"),
            OffsetInterval: offsetInterval,
            TargetJsonSchema: rawExtractAttributes.Get("target_json_schema"),
            HashSalt: stream.GetString("hash_salt"),
            HashAlgo: stream.GetString("hash_algo"),
        }
    }
    return
}

func (job *AquiferJob) getLockPrefix() string {
	if job.jobType == "file" {
		return "file"
	} else if job.jobType == "data-batch" {
		return "data"
	} else if job.jobType == "snapshot" {
		return "snapshot"
	} else {
		return "job"
	}
	return ""
}

func (job *AquiferJob) getJobPath() string {
	var pathType string
	if job.jobType == "file" {
		pathType = "files"
	} else if job.jobType == "data-batch" {
		pathType = "data"
	} else if job.jobType == "snapshot" {
		pathType = "snapshots"
	} else {
		pathType = "jobs"
	}
	return fmt.Sprintf("/accounts/%s/%s/%s",
					   job.accountId.String(),
					   pathType,
					   job.jobId.String())
}
