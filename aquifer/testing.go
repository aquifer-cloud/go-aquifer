package aquifer

import (
    "fmt"
    "time"
    "bytes"
    "context"
    "net/http"
    "encoding/json"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/jarcoal/httpmock"
)

func NewMockDb() map[string]map[string]interface{} {
    return map[string]map[string]interface{}{
        "datastores": map[string]interface{}{},
        "data": map[string]interface{}{},
        "snapshots": map[string]interface{}{},
        "files": map[string]interface{}{},
        "jobs": map[string]interface{}{},
        "extracts": map[string]interface{}{},
    }
}

func NewMockService(mockDb map[string]map[string]interface{}) *AquiferService {
    service := NewService("test-service")
    httpmock.ActivateNonDefault(service.httpClient.GetClient())

    httpmock.RegisterResponder(
        "POST",
        "=~.*/accounts/.*/token",
        func(req *http.Request) (*http.Response, error) {
            return httpmock.NewJsonResponse(
                200,
                map[string]interface{}{
                    "data": map[string]interface{}{
                        "attributes": map[string]interface{}{
                            "deployment_entity_token": "foo",
                        },
                    },
                })
        })

    httpmock.RegisterResponder(
        "POST",
        "=~.*/accounts/([^/]+)/([^/]+)/([^/]+)/lock",
        func(req *http.Request) (*http.Response, error) {
            return httpmock.NewJsonResponse(
                200,
                map[string]interface{}{
                    "data": map[string]interface{}{
                        "attributes": map[string]interface{}{
                            "worker_lock_id": uuid.New().String(),
                        },
                    },
                })
        })

    // httpmock.RegisterResponder(
    //     "GET",
    //     "=~.*/accounts/([^/]+)/(data|jobs|files)/([^/]+)",
    //     func(req *http.Request) (*http.Response, error) {
    //         return httpmock.NewJsonResponse(
    //             200,
    //             map[string]interface{}{
    //                 "data": map[string]interface{}{
    //                     "attributes": map[string]interface{}{
    //                     },
    //                 },
    //             })
    //     })

    httpmock.RegisterResponder(
        "GET",
        "=~.*/accounts/([^/]+)/jobs/([^/]+)/extracts.*",
        func(req *http.Request) (resp *http.Response, err error) {
            var jobId string
            jobId, err = httpmock.GetSubmatch(req, 2)
            if err != nil {
                return
            }

            var found bool
            var extracts interface{}
            extracts, found = mockDb["extracts"][jobId]
            if !found {
                return httpmock.NewJsonResponse(
                    404,
                    map[string]interface{}{})
            }

            return httpmock.NewJsonResponse(200, extracts)
        })

    httpmock.RegisterResponder(
        "GET",
        "=~.*/accounts/([^/]+)/(data|snapshots|jobs|files|datastores|blobstores|integrations|processors)/([^/]+)",
        func(req *http.Request) (resp *http.Response, err error) {
            var entityType string
            entityType, err = httpmock.GetSubmatch(req, 2)
            if err != nil {
                return
            }

            var entityId string
            entityId, err = httpmock.GetSubmatch(req, 3)
            if err != nil {
                return
            }

            var found bool
            var entities map[string]interface{}
            var entity interface{}
            entities, found = mockDb[entityType]
            if found {
                entity, found = entities[entityId]
            }
            if !found {
                return httpmock.NewJsonResponse(
                    404,
                    map[string]interface{}{})
            }

            return httpmock.NewJsonResponse(
                200,
                map[string]interface{}{
                    "data": map[string]interface{}{
                        "id": entityId,
                        "attributes": entity,
                    },
                })
        })

    httpmock.RegisterResponder(
        "POST",
        "=~.*/accounts/([^/]+)/(data|files)/upload$",
        func(req *http.Request) (*http.Response, error) {
            body := make([]byte, req.ContentLength)
            req.Body.Read(body)
            var data map[string]interface{}
            json.Unmarshal(body, &data)

            attributes := data["data"].(map[string]interface{})["attributes"].(map[string]interface{})
            uploadToken := attributes["upload_token"]
            var fileId string
            if uploadToken != nil {
                fileId = uploadToken.(string)
            } else {
                fileId = uuid.New().String()
            }

            uploadUrl := "https://examples.com/files/" + fileId
            httpmock.RegisterResponder(
                "PUT",
                uploadUrl,
                func(req *http.Request) (*http.Response, error) {
                    body := make([]byte, req.ContentLength)
                    req.Body.Read(body)
                    mockDb["files"][fileId] = body

                    resp := httpmock.NewStringResponse(200, "OK")
                    resp.Header.Add("Etag", "foo")
                    return resp, nil
                })

            return httpmock.NewJsonResponse(
                200,
                map[string]interface{}{
                    "data": map[string]interface{}{
                        "id": fileId,
                        "attributes": map[string]interface{}{
                            "upload_token": fileId,
                            "part_number": 1,
                            "upload_url": uploadUrl,
                        },
                    },
                })
        })

    httpmock.RegisterResponder(
        "POST",
        "=~.*/accounts/([^/]+)/(data|files)/upload/complete",
        func(req *http.Request) (*http.Response, error) {
            body := make([]byte, req.ContentLength)
            req.Body.Read(body)
            var data map[string]interface{}
            json.Unmarshal(body, &data)

            entity := data["data"].(map[string]interface{})["attributes"].(map[string]interface{})
            entity["id"] = entity["upload_token"]

            mockDb["data"][entity["upload_token"].(string)] = entity

            return httpmock.NewJsonResponse(
                200,
                map[string]interface{}{
                    "data": map[string]interface{}{
                        "attributes": entity,
                    },
                })
        })

    httpmock.RegisterResponder(
        "POST",
        "=~.*/accounts/([^/]+)/(data|files)",
        func(req *http.Request) (*http.Response, error) {
            entityType, err := httpmock.GetSubmatch(req, 2)
            if err != nil {
                return nil, err
            }

            entityId := uuid.New().String()

            body := make([]byte, req.ContentLength)
            req.Body.Read(body)
            var data map[string]interface{}
            json.Unmarshal(body, &data)
            entity := data["data"].(map[string]interface{})["attributes"].(map[string]interface{})
            entity["id"] = entityId

            mockDb[entityType][entityId] = entity

            return httpmock.NewJsonResponse(200, data)
        })

    // httpmock.RegisterNoResponder(
    //     httpmock.NewStringResponder(500, "Mock route not found"))

    return service
}

type MockDataBatch struct {
    data []map[string]interface{}
    id uuid.UUID
    sequence int
    snapshotVersion int
    idempotentId string
    hyperbatchId uuid.UUID
    stateSequence int
    relativePath string
    jsonSchema map[string]interface{}
    maxCount int
    maxByteSize int
    recordsBuffer bytes.Buffer
}

func NewMockDataBatch(relativePath string,
                      jsonSchema map[string]interface{},
                      data []map[string]interface{},
                      snapshotVersion int,
                      hyperbatchId uuid.UUID) *MockDataBatch {
    id := uuid.New()
    mockDataBatch := MockDataBatch{
        id: id,
        relativePath: relativePath,
        jsonSchema: jsonSchema,
        idempotentId: id.String(),
        data: data,
        sequence: int(time.Now().Unix()),
        snapshotVersion: snapshotVersion,
        hyperbatchId: hyperbatchId,
    }
    return &mockDataBatch
}

func (databatch *MockDataBatch) GetId() uuid.UUID {
    return databatch.id
}

func (databatch *MockDataBatch) GetRelativePath() string {
    return databatch.relativePath
}

func (databatch *MockDataBatch) GetSequence() int {
    return databatch.sequence
}

func (databatch *MockDataBatch) GetSnapshotVersion() int {
    return databatch.snapshotVersion
}

func (databatch *MockDataBatch) GetIdempotentId() string {
    return databatch.idempotentId
}

func (databatch *MockDataBatch) GetHyperbatchId() uuid.UUID {
    return databatch.hyperbatchId
}

func (databatch *MockDataBatch) GetJsonSchema() map[string]interface{} {
    return databatch.jsonSchema
}

func (databatch *MockDataBatch) GetCount() int {
    return len(databatch.data)
}

func (databatch *MockDataBatch) IsFull() bool {
    return false
}

func (databatch *MockDataBatch) AddRecord(record map[string]interface{}, stateSequence uint64) (err error) {
    return
}

func (databatch *MockDataBatch) Complete() (err error) {
    return
}

func (databatch *MockDataBatch) Cancel() error {
    return nil
}

func (databatch *MockDataBatch) NextRecord() (record map[string]interface{}, exists bool, err error) {
    if len(databatch.data) == 0 {
        return
    }
    record = databatch.data[0]
    exists = true
    databatch.data = databatch.data[1:]
    return
}

type MockJob struct {
    service *AquiferService
    event AquiferEvent
    accountId uuid.UUID
    jobType string
    jobId uuid.UUID
    flowId uuid.UUID
    entityTypeName string
    entityType string
    entityId uuid.UUID
    config Dict
    databatch DataBatchInterface
}

func NewMockJob(config map[string]interface{}, service *AquiferService) *MockJob {
    return &MockJob{
        service: service,
        config: config,
    }
}

func (job *MockJob) Logger() *zerolog.Logger {
    return log.Ctx(job.GetCtx())
}

func (job *MockJob) GetCtx() context.Context {
    return context.TODO()
}

func (job *MockJob) GetService() *AquiferService {
    return nil
}

func (job *MockJob) GetEvent() AquiferEvent {
    return job.event
}

func (job *MockJob) IsTimedout() bool {
    return false
}

func (job *MockJob) GetAccountId() uuid.UUID {
    return job.accountId
}

func (job *MockJob) GetType() string {
    return job.jobType
}

func (job *MockJob) GetId() uuid.UUID {
    return job.jobId
}

func (job *MockJob) GetFlowId() uuid.UUID {
    return job.flowId
}

func (job *MockJob) GetEntityTypeName() string {
    return job.entityTypeName
}

func (job *MockJob) GetEntityType() string {
    return job.entityType
}

func (job *MockJob) SetEntityType(entityType string) {
    job.entityType = entityType
}

func (job *MockJob) GetEntityId() uuid.UUID {
    return job.entityId
}

func (job *MockJob) GetConfig() Dict {
    return job.config
}

func (job *MockJob) Lock() (err error) {
    return
}

func (job *MockJob) Release(releaseStatus string, failureErrorId uuid.UUID) (err error) {
    return
}

func (job *MockJob) Touch() (err error) {
    return
}

func (job *MockJob) GetJobAttributes() Dict {
    return Dict{}
}

func (job *MockJob) GetRelativePath() string {
    return ""
}

func (job *MockJob) GetSnapshotVersion() int {
    return 0
}

func (job *MockJob) GetHyperbatchId() uuid.UUID {
    return uuid.Nil
}

func (job *MockJob) SetDataBatch(databatch DataBatchInterface) {
    job.databatch = databatch
}

func (job *MockJob) GetDataBatch() DataBatchInterface {
    return job.databatch
}

func (job *MockJob) GetDataOutputStream() *DataOutputStream {
    return nil
}

func (job *MockJob) GetFlow() (flow *Flow, err error) {
    return
}

func (job *MockJob) GetExtracts() (extracts []*Extract, err error) {
    err = fmt.Errorf("GetExtracts not implemented")
    return
}

func (job *MockJob) UpsertSchema(relativePath string,
                                 schema map[string]interface{}) (upsertedSchema map[string]interface{}, err error) {
    return
}
