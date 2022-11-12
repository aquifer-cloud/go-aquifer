package aquifer

import (
    "fmt"
    "testing"

    "github.com/rs/zerolog/log"
    "golang.org/x/sync/errgroup"
    "github.com/elliotchance/orderedmap/v2"
)

func TestState(t *testing.T) {
    var err error
    config := make(map[string]interface{})
    job := NewMockJob(config)
    logger := log.With().Logger()
    dataOutputStream := &DataOutputStream{
        ctx: job.GetCtx(),
        job: job,
        logger: &logger,
        metricsSource: "extract",
        schemas: make(map[string](map[string]interface{})),
        states: orderedmap.NewOrderedMap[uint64, map[string]interface{}](),
        messageSequence: 1, // instead of usual 0
    }

    err = dataOutputStream.FetchState()
    if err != nil {
        fmt.Println(err)
        return
    }

    dataOutputStream.SetSchema("project", map[string]interface{}{
        "type": "object",
        "additionalProperties": false,
    })

    outputChan := make(chan OutputMessage, 10)

    outputWg, outputWgCtx := errgroup.WithContext(job.GetCtx())
    outputWg.Go(func() error {
        return dataOutputStream.Worker(outputWgCtx, outputChan)
    })

    dataOutputStream.states.Set(1, map[string]interface{}{
        "updated_at": " 2020-11-12T21:17:19Z",
    })

    var message OutputMessage
    for i := 1; i < 5; i++ {
        message = OutputMessage{
            Type: Record,
            RelativePath: "project",
            Data: map[string]interface{}{
                "id": 1,
                "name": "Foo",
            },
        }
        outputChan <- message
    }

    message = OutputMessage{
        Type: StateUpdate,
        RelativePath: "project",
        Data: map[string]interface{}{
            "updated_at": " 2022-11-12T21:17:19Z",
        },
    }
    outputChan <- message

    fmt.Println(dataOutputStream.GetState())
    fmt.Println(dataOutputStream.GetNextState())

    close(outputChan)

    err = outputWg.Wait()
    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Println(dataOutputStream.GetState())
    fmt.Println(dataOutputStream.GetNextState())

    err = dataOutputStream.FlushState(false)
    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Println(dataOutputStream.GetState())
    fmt.Println(dataOutputStream.GetNextState())
}
