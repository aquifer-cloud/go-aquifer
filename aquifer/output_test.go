package aquifer

// import (
//     "fmt"
//     "testing"
//     "net/http"

//     "github.com/google/uuid"
//     "github.com/rs/zerolog/log"
//     "golang.org/x/sync/errgroup"
//     "github.com/jarcoal/httpmock"
//     "github.com/elliotchance/orderedmap/v2"
// )

// func TestState(t *testing.T) {
//     var err error
//     config := make(map[string]interface{})
//     service := NewMockService()
//     job := NewMockJob(config, service)
//     logger := log.With().Logger()
//     dataOutputStream := &DataOutputStream{
//         service: service,
//         ctx: job.GetCtx(),
//         job: job,
//         logger: &logger,
//         entityType: "integration",
//         metricsSource: "extract",
//         schemas: make(map[string](map[string]interface{})),
//         states: orderedmap.NewOrderedMap[uint64, map[string]interface{}](),
//         messageSequence: 1, // instead of usual 0
//     }

//     httpmock.RegisterResponder(
//         "GET",
//         "=~.*/accounts/.*/state",
//         func(req *http.Request) (*http.Response, error) {
//             return httpmock.NewJsonResponse(
//                 200,
//                 map[string]interface{}{
//                     "data": map[string]interface{}{
//                         "attributes": map[string]interface{}{
//                             "state": map[string]interface{}{
//                                 "foo": "bar",
//                             },
//                         },
//                     },
//                 })
//         })

//     httpmock.RegisterResponder(
//         "POST",
//         "=~.*/accounts/.*/state",
//         func(req *http.Request) (*http.Response, error) {
//             return httpmock.NewJsonResponse(
//                 200,
//                 map[string]interface{}{
//                     "data": map[string]interface{}{
//                         "attributes": map[string]interface{}{
//                         },
//                     },
//                 })
//         })

//     httpmock.RegisterResponder(
//         "POST",
//         "=~.*/accounts/.*/data/upload",
//         func(req *http.Request) (*http.Response, error) {
//             return httpmock.NewJsonResponse(
//                 200,
//                 map[string]interface{}{
//                     "data": map[string]interface{}{
//                         "id": uuid.New().String(),
//                         "attributes": map[string]interface{}{
//                             "upload_token": "mock-upload-token",
//                             "part_number": 1,
//                             "upload_url": "https://example.com/upload",
//                         },
//                     },
//                 })
//         })

//     httpmock.RegisterResponder(
//         "PUT",
//         "https://example.com/upload",
//         func(req *http.Request) (*http.Response, error) {
//             response := httpmock.NewStringResponse(200, "Foo")
//             response.Header.Set("Etag", "fake-etag")
//             return response, nil
//         })

//     err = dataOutputStream.FetchState()
//     if err != nil {
//         fmt.Println(err)
//         return
//     }

//     dataOutputStream.SetSchema("project", map[string]interface{}{
//         "type": "object",
//         "additionalProperties": false,
//     })

//     outputChan := make(chan OutputMessage, 10)

//     outputWg, outputWgCtx := errgroup.WithContext(job.GetCtx())
//     outputWg.Go(func() error {
//         return dataOutputStream.Worker(outputWgCtx, outputChan)
//     })

//     dataOutputStream.states.Set(1, map[string]interface{}{
//         "updated_at": " 2020-11-12T21:17:19Z",
//     })

//     var message OutputMessage
//     for i := 1; i < 5; i++ {
//         message = OutputMessage{
//             Type: Record,
//             RelativePath: "project",
//             Data: map[string]interface{}{
//                 "id": 1,
//                 "name": "Foo",
//             },
//         }
//         outputChan <- message
//     }

//     message = OutputMessage{
//         Type: StateUpdate,
//         RelativePath: "project",
//         Data: map[string]interface{}{
//             "updated_at": " 2022-11-12T21:17:19Z",
//         },
//     }
//     outputChan <- message

//     fmt.Println(dataOutputStream.GetState())
//     fmt.Println(dataOutputStream.GetNextState())

//     close(outputChan)

//     err = outputWg.Wait()
//     if err != nil {
//         fmt.Println(err)
//         return
//     }

//     fmt.Println(dataOutputStream.GetState())
//     fmt.Println(dataOutputStream.GetNextState())

//     err = dataOutputStream.FlushState(false)
//     if err != nil {
//         fmt.Println(err)
//         return
//     }

//     fmt.Println(dataOutputStream.GetState())
//     fmt.Println(dataOutputStream.GetNextState())
// }
