package aquifer

import (
    "fmt"
    "time"
    "bytes"
    "context"
    "net/http"

    "github.com/google/uuid"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/jarcoal/httpmock"
)

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
            // httpmock.MustGetSubmatch(req, )
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
        "=~.*/accounts/([^/]+)/(data|jobs|files|datastores|blobstores|integrations|processors)/([^/]+)",
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

func (job *MockJob) SetDataBatch(databatch DataBatchInterface) {
    job.databatch = databatch
}

func (job *MockJob) GetDataBatch() DataBatchInterface {
    return job.databatch
}

func (job *MockJob) GetDataOutputStream() *DataOutputStream {
    return nil
}

func (job *MockJob) GetExtracts() (extracts []*Extract, err error) {
    err = fmt.Errorf("GetExtracts not implemented")
    return
}
