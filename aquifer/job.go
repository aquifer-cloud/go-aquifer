package aquifer

import (
    "io"
    "fmt"
    "time"
    "sync"
    "context"
    netURL "net/url"

    "github.com/google/uuid"
    "golang.org/x/exp/slices"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/relvacode/iso8601"
)

type EventSource struct {
    Type string `json:"type"`
    Id *uuid.UUID `json:"id,omitempty"`
    FlowId *uuid.UUID `json:"flow_id,omitempty"`
    JobType string `json:"job_type,omitempty"`
    JobId *uuid.UUID `json:"job_id,omitempty"`
    HyperbatchId *uuid.UUID `json:"hyperbatch_id,omitempty"`
    ResponseHandle string `json:"response_handle,omitempty"`
}

type EventDestination struct {
    Type string `json:"type"`
    Id *uuid.UUID `json:"id"`
    FlowId *uuid.UUID `json:"flow_id,omitempty"`
    JobType string `json:"job_type,omitempty"`
    JobId *uuid.UUID `json:"job_id,omitempty"`
    Handle string `json:"handle,omitempty"`
    ResponseHandle string `json:"response_handle,omitempty"`
}

type AquiferTime struct {
    time.Time
}

func ToAquiferTime(input time.Time) AquiferTime {
    return AquiferTime{
        Time: input,
    }
}

func (t *AquiferTime) UnmarshalJSON(b []byte) (err error) {
    t.Time, err = iso8601.ParseString(string(b[1:len(b)-1]))
    return
}

type AquiferEvent struct {
    Id uuid.UUID `json:"id"`
    Type string `json:"type"`
    AccountId uuid.UUID `json:"account_id"`
    Timestamp AquiferTime `json:"timestamp"`
    Source EventSource `json:"source,omitempty"`
    Destination EventDestination `json:"destination,omitempty"`
    Payload Dict `json:"payload,omitempty"`
}

type JobInterface interface {
    Logger() *zerolog.Logger
    GetCtx() context.Context
    GetEvent() AquiferEvent
    GetService() *AquiferService
    IsTimedout() bool
    GetAccountId() uuid.UUID
    GetType() string
    GetId() *uuid.UUID
    GetFlowId() *uuid.UUID
    GetEntityTypeName() string
    GetEntityType() string
    GetEntityId() *uuid.UUID
    GetConfig() Dict
    GetRelativePath() string
    GetSnapshotVersion() int
    GetHyperbatchId() *uuid.UUID
    SetHyperbatchId(uuid.UUID)
    GetJobAttributes() Dict
    GetDataBatch() DataBatchInterface
    GetDataOutputStream(JobOutputStreamOptions) *DataOutputStream
    Lock() error
    Release(releaseStatus string, failureErrorId *uuid.UUID) error
    Touch() error
    GetFlow() (*Flow, error)
    GetExtracts() ([]*Extract, error)
    UpsertSchema(string, map[string]interface{}) (map[string]interface{}, error)
    NewEvent(string, EventDestination, Dict) *AquiferEvent
    SendResponse(*AquiferEvent) error
    CreateFile() *AquiferFile
    CreateFileDownload(string, string, Dict, Dict) error
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
    entityId *uuid.UUID
    flowId *uuid.UUID
    hyperbatchId *uuid.UUID
    jobType string
    jobId *uuid.UUID
    relativePath string
    jobAttributes Dict
    entityCatalogAttributes Dict
    entityAttributes Dict
    lockId *uuid.UUID
    locked bool
    timeout_sec int
    lastTouch time.Time
    touchLock sync.Mutex
    ackImmediately bool
}

func NewAquiferJobFromEvent(service *AquiferService, ctx context.Context, event AquiferEvent) (job JobInterface, err error) {
    if event.Destination.JobType == "file" || event.Destination.JobType == "data-batch" {
        job = NewFileJobFromEvent(service, ctx, event)
    } else if event.Type == "query" ||
        slices.Contains([]string{"extract", "schema-sync", "snapshot", "connection-test"}, event.Destination.JobType) {
        job = NewJobFromEvent(service, ctx, event)
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
            hyperbatchId: event.Source.HyperbatchId,
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

func NewJobFromEvent(service *AquiferService, ctx context.Context, event AquiferEvent) JobInterface {
    jobCtx, jobCancel := context.WithCancel(ctx)

    loggerParams := log.With().
        Str("account_id", event.AccountId.String()).
        Str("entity_type", event.Destination.Type).
        Str("entity_id", event.Destination.Id.String())

    var jobType string
    if event.Destination.JobType != "" {
        loggerParams = loggerParams.
            Str("job_type", event.Destination.JobType).
            Str("job_id", event.Destination.JobId.String())
        jobType = event.Destination.JobType
    } else {
        jobType = event.Type
    }

    if event.Destination.FlowId != nil {
        loggerParams = loggerParams.Str("flow_id", event.Destination.FlowId.String())
    }

    ackImmediately := false
    if slices.Contains([]string{"extract", "schema-sync", "connection-test"}, jobType) {
        ackImmediately = true
    }

    jobCtx = loggerParams.Logger().WithContext(jobCtx)
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
        jobType: jobType,
        jobId: event.Destination.JobId,
        ackImmediately: ackImmediately,
        hyperbatchId: event.Source.HyperbatchId,
    }
}

func NewJobFromCLI(service *AquiferService,
                   ctx context.Context,
                   jobType string,
                   accountIdStr string,
                   flowIdStr string,
                   entityType string,
                   entityIdStr string,
                   jobIdStr string) (job JobInterface, err error) {
    var accountId uuid.UUID
    accountId, err = uuid.Parse(accountIdStr)
    if err != nil {
        return
    }

    var entityId uuid.UUID
    entityId, err = uuid.Parse(entityIdStr)
    if err != nil {
        return
    }

    loggerParams := log.With().
        Str("account_id", accountIdStr).
        Str("entity_type", entityType).
        Str("entity_id", entityIdStr)

    var flowId uuid.UUID
    if flowIdStr != "" {
        flowId, err = uuid.Parse(flowIdStr)
        if err != nil {
            return
        }
        loggerParams = loggerParams.Str("flow_id", flowIdStr)
    }

    var jobId uuid.UUID
    if jobIdStr != "" {
        jobId, err = uuid.Parse(jobIdStr)
        if err != nil {
            return
        }
        loggerParams = loggerParams.Str("job_id", jobIdStr)
    }

    jobCtx, jobCancel := context.WithCancel(ctx)
    jobCtx = loggerParams.Logger().WithContext(jobCtx)
    logger := log.Ctx(jobCtx)

    jobVal := AquiferJob{
        service: service,
        ctx: jobCtx,
        logger: logger,
        accountId: accountId,
        entityType: entityType,
        entityId: &entityId,
        jobType: jobType,
        cancel: jobCancel,
        timeout_sec: 3600,
    }

    if flowIdStr != "" {
        jobVal.flowId = &flowId
    }

    if jobIdStr != "" {
        jobVal.jobId = &jobId
    }

    if jobType == "data-batch" || jobType == "file" {
        job = FileJob{
            AquiferJob: &jobVal,
            File: NewAquiferFile(
                service,
                jobCtx,
                "rb",
                false, // On read, this comes from the API response
                jobType,
                accountId,
                entityType,
                &entityId,
                &jobId),
        }
    } else {
        job = &jobVal
    }
    return
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

func (job *AquiferJob) GetEvent() AquiferEvent {
    return job.event
}

func (job *AquiferJob) GetType() string {
    return job.jobType
}

func (job *AquiferJob) GetId() *uuid.UUID {
    return job.jobId
}

func (job *AquiferJob) GetFlowId() *uuid.UUID {
    return job.flowId
}

func (job *AquiferJob) GetEntityTypeName() string {
    return job.entityAttributes.GetString("type_name")
}

func (job *AquiferJob) GetEntityType() string {
    return job.entityType
}

func (job *AquiferJob) GetEntityId() *uuid.UUID {
    return job.entityId
}

func (job *AquiferJob) GetAccountId() uuid.UUID {
    return job.accountId
}

func (job *AquiferJob) GetRelativePath() string {
    return job.jobAttributes.GetString("relative_path")
}

func (job *AquiferJob) GetSnapshotVersion() int {
    snapshotVersion, _ := job.jobAttributes.GetInt("snapshot_version")
    return snapshotVersion
}

func (job *AquiferJob) GetHyperbatchId() *uuid.UUID {
    if job.hyperbatchId == nil {
        hyperbatchIdStr := job.jobAttributes.GetString("hyperbatch_id")
        if hyperbatchIdStr == "" {
            return nil
        }
        hyperbatchId, _ := uuid.Parse(hyperbatchIdStr)
        job.hyperbatchId = &hyperbatchId
    }

    return job.hyperbatchId
}

func (job *AquiferJob) SetHyperbatchId(hyperbatchId uuid.UUID) {
    job.hyperbatchId = &hyperbatchId
}

func (job *AquiferJob) IsTimedout() bool {
    return job.timedout
}

func (job *AquiferJob) GetTimeout() time.Duration {
    return time.Duration(job.timeout_sec) * time.Second
}

func (job *AquiferJob) GetConfig() Dict {
    return job.entityCatalogAttributes.Get("config").Merge(
        job.entityCatalogAttributes.Get("secrets").Merge(
            job.entityAttributes.Get("config")))
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

type JobOutputStreamOptions struct {
    AllowDiscovery bool
    EnableTransform bool
    MaxBatchCount int
    MaxBatchByteSize int
}

func (job *AquiferJob) GetDataOutputStream(options JobOutputStreamOptions) *DataOutputStream {
    job.dataOutputStreamLock.Lock()
    defer job.dataOutputStreamLock.Unlock()

    if job.dataOutputStream == nil {
        source := make(Dict).
            SetString("type", job.GetEntityType()).
            SetString("id", job.GetEntityId().String()).
            SetString("flow_id", job.GetFlowId().String()).
            SetString("job_type", job.GetType())

        if job.GetId() != nil {
            source = source.SetString("job_id", job.GetId().String())
        }

        if job.GetHyperbatchId() != nil {
            source = source.SetString("hyperbatch_id", job.GetHyperbatchId().String())
        }

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
            job.GetEntityType(),
            job.GetEntityId(),
            metricsSource,
            options.AllowDiscovery,
            options.EnableTransform,
            options.MaxBatchCount,
            options.MaxBatchByteSize)
    }

    return job.dataOutputStream
}

func (job *AquiferJob) Lock() (err error) {
    var token string
    token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
    if err != nil {
        return
    }

    if job.jobId != nil {
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
            RequestOptions{
                Token: token,
                Body: reqData,
            })
        if err != nil {
            return
        }

        rawLockId := data.Get("data").Get("attributes").GetString("worker_lock_id")
        var lockId uuid.UUID
        lockId, err = uuid.Parse(rawLockId)
        if err != nil {
            return
        }
        job.lockId = &lockId

        if job.jobType == "extract" || job.jobType == "schema-sync" {
            job.jobAttributes = data.Get("data").Get("attributes")
        } else {
            var jobData Dict
            jobData, err = job.service.Request(
                job.ctx,
                "GET",
                job.getJobPath(),
                RequestOptions{
                    Token: token,
                })
            if err != nil {
                return
            }

            job.jobAttributes = jobData.Get("data").Get("attributes")
        }

        var exists bool
        job.timeout_sec, exists = job.jobAttributes.GetInt("timeout")
        if !exists {
            job.timeout_sec = 900
        }
    } else if job.event.Destination.Handle != "" {
        reqData := make(Dict).
            Set("data", make(Dict).
                SetString("type", "deployment-event-touch").
                Set("attributes", make(Dict).
                    SetString("handle", job.event.Destination.Handle)))

        _, err = job.service.Request(
            job.ctx,
            "POST",
            fmt.Sprintf("/deployments/%s/events/touch", job.service.deploymentName),
            RequestOptions{
                Body: reqData,
            })
        if err != nil {
            return
        }
        job.timeout_sec = 60
    }

    job.locked = true
    job.lastTouch = time.Now()

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
        RequestOptions{
            Token: token,
        })
    if err != nil {
        return
    }

    job.entityAttributes = entityData.Get("data").Get("attributes")

    var path string
    if job.service.GetIsAccountDeployment() {
        path = fmt.Sprintf(
            "/accounts/%s/catalog-types/%s/%s",
            job.accountId,
            job.entityType,
            job.entityAttributes.GetString("type_name"))
    } else {
        path = fmt.Sprintf(
            "/catalog-types/%s/%s",
            job.entityType,
            job.entityAttributes.GetString("type_name"))
    }

    var entityCatalogData Dict
    entityCatalogData, err = job.service.Request(
        job.ctx,
        "GET",
        path,
        RequestOptions{})
    if err != nil {
        return
    }

    job.entityCatalogAttributes = entityCatalogData.Get("data").Get("attributes")

    return
}

func (job *AquiferJob) Release(releaseStatus string, failureErrorId *uuid.UUID) (err error) {
    defer func() {
        if job.cancelTimer != nil {
            job.cancelTimer.Stop() // TODO: needed?
        }
        job.cancel()
    }()

    if job.locked {
        if job.jobId != nil {
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

            if failureErrorId != nil {
                attributes = attributes.SetString("failure_error_id", failureErrorId.String())
            }

            if !job.ackImmediately && job.event.Destination.Handle != "" {
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
                RequestOptions{
                    Token: token,
                    Body: reqData,
                })
            if err != nil {
                return
            }
            job.locked = false
        } else {
            if releaseStatus == "failed" {
                requestSource := job.GetEvent().Source
                eventDestination := EventDestination{
                    Type: requestSource.Type,
                    Id: requestSource.Id,
                    ResponseHandle: requestSource.ResponseHandle,
                }

                payload := make(map[string]interface{})
                payload["http_status_code"] = 500
                payload["http_status_text"] = "Internal Error"

                event := job.NewEvent("error-response", eventDestination, payload)

                // we ignore SendResponse errors so the event message gets deleted,
                // the response will eventually timeout
                job.SendResponse(event)
                if err != nil {
                    job.Logger().Error().Err(err).Msg("Error responding to request event")
                }
            }

            reqData := make(Dict).
                Set("data", make(Dict).
                    SetString("type", "deployment-event").
                    Set("attributes", make(Dict).
                        SetString("handle", job.event.Destination.Handle)))

            _, err = job.service.Request(
                job.ctx,
                "DELETE",
                fmt.Sprintf("/deployments/%s/events", job.service.deploymentName),
                RequestOptions{
                    Body: reqData,
                })
            if err != nil {
                return
            }
        }
        job.locked = false
    }
    return
}

func (job *AquiferJob) Touch() (err error) {
    job.touchLock.Lock()
    defer job.touchLock.Unlock()

    if job.locked && time.Now().Sub(job.lastTouch).Seconds() >= 60.0 {
        if job.jobId != nil {
            var token string
            token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
            if err != nil {
                return
            }

            attributes := make(Dict)

            if !job.ackImmediately && job.event.Destination.Handle != "" {
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
                RequestOptions{
                    Token: token,
                    Body: reqData,
                })
            if err != nil {
                return
            }
        } else if job.event.Destination.Handle != "" {
            reqData := make(Dict).
                Set("data", make(Dict).
                    SetString("type", "deployment-event-touch").
                    Set("attributes", make(Dict).
                        SetString("handle", job.event.Destination.Handle)))

            _, err = job.service.Request(
                job.ctx,
                "POST",
                fmt.Sprintf("/deployments/%s/events/touch", job.service.deploymentName),
                RequestOptions{
                    Body: reqData,
                })
            if err != nil {
                return
            }
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

type Flow struct {
    Id uuid.UUID
    Name string
}

func (job *AquiferJob) GetFlow() (flow *Flow, err error) {
    var token string
    token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
    if err != nil {
        return
    }

    var data Dict
    data, err = job.service.Request(
        job.ctx,
        "GET",
        fmt.Sprintf("/accounts/%s/flows/%s",
            job.accountId,
            job.flowId),
        RequestOptions{
            Token: token,
        })
    if err != nil {
        return
    }

    flow = &Flow{
        Id: *job.flowId,
        Name: data.Get("data").Get("attributes").GetString("name"),
    }
    return
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
        fmt.Sprintf("/accounts/%s/flows/%s/extracts?include=stream,schema&filter[entity_type]=%s&filter[entity_id]=%s",
            job.GetAccountId().String(),
            job.GetFlowId().String(),
            job.GetEntityType(),
            job.GetEntityId().String()),
        RequestOptions{
            Token: token,
        })
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
        rawExtractDict := Dict(rawExtract.(map[string]interface{}))
        rawExtractAttributes := rawExtractDict.Get("attributes")
        offsetInterval, _ := rawExtractAttributes.GetFloat64("offset_interval")

        streamId := rawExtractDict.
            Get("relationships").
            Get("stream").
            Get("data").
            GetString("id")
        stream := Dict(includes["stream"][streamId].(map[string]interface{}))
        streamAttributes := stream.Get("attributes")
        schemaId := stream.
            Get("relationships").
            Get("schema").
            Get("data").
            GetString("id")
        schema := Dict(includes["schema"][schemaId].(map[string]interface{}))
        schemaAttributes := schema.Get("attributes")

        extracts[i] = &Extract{
            Path: schemaAttributes.GetString("path"),
            RelativePath: schemaAttributes.GetString("relative_path"),
            ExtractType: rawExtractAttributes.GetString("extract_type"),
            OffsetField: rawExtractAttributes.GetString("offset_field"),
            OffsetInterval: offsetInterval,
            TargetJsonSchema: rawExtractAttributes.Get("target_json_schema"),
            HashSalt: streamAttributes.GetString("hash_salt"),
            HashAlgo: streamAttributes.GetString("hash_algo"),
        }
    }
    return
}

func (job *AquiferJob) UpsertSchema(relativePath string,
                                    schema map[string]interface{}) (upsertedSchema map[string]interface{}, err error) {
    var entityPath string
    entityPath, err = job.service.GetEntityPath(
        job.accountId,
        job.entityType,
        job.entityId)
    if err != nil {
        return
    }

    var token string
    token, err = job.service.GetEntityToken(job.ctx, job.accountId, job.entityType, job.entityId)
    if err != nil {
        return
    }

    reqData := make(Dict).
        Set("data", make(Dict).
            SetString("type", "schema").
            Set("attributes", schema))

    var data Dict
    data, err = job.service.Request(
        job.GetCtx(),
        "PUT",
        fmt.Sprintf("%s/schemas/relative-paths/%s", entityPath, relativePath),
        RequestOptions{
            Token: token,
            Body: reqData,
        })
    if err != nil {
        return
    }

    upsertedSchema = data.Get("data").Get("attributes")
    return
}

func (job *AquiferJob) NewEvent(eventType string,
                                destination EventDestination,
                                payload Dict) *AquiferEvent {
    return &AquiferEvent{
        Id: uuid.New(),
        Type: eventType,
        AccountId: job.GetAccountId(),
        Timestamp: ToAquiferTime(time.Now().UTC()),
        Source: EventSource{
            Type: job.GetEntityType(),
            Id: job.GetEntityId(),
            FlowId: job.GetFlowId(),
            JobType: job.GetType(),
            JobId: job.GetId(),
            HyperbatchId: job.GetHyperbatchId(),
        },
        Destination: destination,
        Payload: payload,
    }
}

func (job *AquiferJob) SendResponse(event *AquiferEvent) (err error) {
    reqData := make(Dict).
        Set("data", make(Dict).
            SetString("type", "event").
            SetAny("attributes", event))

    _, err = job.service.Request(
        job.GetCtx(),
        "POST",
        fmt.Sprintf("/deployments/%s/events", job.service.deploymentName),
        RequestOptions{
            Body: reqData,
        })
    return
}

func (job *AquiferJob) CreateFile() *AquiferFile {
    id := uuid.New()
    return NewAquiferFile(
        job.service,
        job.GetCtx(),
        "wb",
        false,
        "file",
        job.accountId,
        job.entityType,
        job.entityId,
        &id)
}

func (job *AquiferJob) CreateFileDownload(relativePath string,
                                          url string,
                                          metadata Dict,
                                          metadataSchema Dict) (err error) {
    var token string
    token, err = job.service.GetEntityToken(job.GetCtx(), job.accountId, job.entityType, job.entityId)
    if err != nil {
        return
    }

    source := make(Dict).
        SetString("type", job.GetEntityType()).
        SetString("id", job.GetEntityId().String()).
        SetString("flow_id", job.GetFlowId().String()).
        SetString("job_type", job.GetType())

    if job.GetId() != nil {
        source = source.SetString("job_id", job.GetId().String())
    }

    if job.GetHyperbatchId() != nil {
        source = source.SetString("hyperbatch_id", job.GetHyperbatchId().String())
    }

    var parsedUrl *netURL.URL
    parsedUrl, err = netURL.Parse(url)
    if err != nil {
        return
    }

    reqData := make(Dict).
        Set("data", make(Dict).
            SetString("type", "file").
            Set("attributes", make(Dict).
                Set("source", source).
                SetString("source_path", parsedUrl.Path).
                SetString("relative_path", relativePath).
                SetString("url", url).
                Set("custom_metadata", metadata).
                Set("custom_metadata_schema", metadataSchema)))

    _, err = job.service.Request(
        job.ctx,
        "POST",
        fmt.Sprintf("/accounts/%s/files", job.accountId),
        RequestOptions{
            Token: token,
            Body: reqData,
        })
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
