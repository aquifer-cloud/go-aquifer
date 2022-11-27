package aquifer

import (
	"os"
	"fmt"
	"time"
	"runtime"
	"context"
	"syscall"
    "os/signal"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "github.com/urfave/cli/v2"
    "github.com/rs/zerolog/pkgerrors"
	"github.com/go-resty/resty/v2"
	"github.com/jellydator/ttlcache/v3"
)

const DEFAULT_BASE_URL string = "https://api.dev.aquifer.cloud"

func getenv(varName string, defaultValue string) string {
	varValue := os.Getenv(varName)
	if defaultValue != "" && varValue == "" {
		varValue = defaultValue
	}
	return varValue
}

type AquiferService struct {
	httpClient *resty.Client
	baseUrl string
	workerId string
	deploymentName string
	deploymentToken string
	tokenCache *ttlcache.Cache[string, string]
	fileHandler func(JobInterface) error
	dataHandler func(JobInterface) error
	extractHandler func(JobInterface) error
}

func NewService(deploymentName string) *AquiferService {
	baseUrl := getenv("AQUIFER_BASE_URL", DEFAULT_BASE_URL)
	deploymentToken := os.Getenv("AQUIFER_TOKEN")
	workerId := getenv("AQUIFER_WORKER_ID",
					   fmt.Sprintf("%s-0", deploymentName))

	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

    httpClient := resty.New().
        SetRetryCount(5).
        SetRetryWaitTime(5 * time.Second).
        SetRetryMaxWaitTime(30 * time.Second).
        AddRetryCondition(
            func(r *resty.Response, err error) bool {
                return err != nil || r.StatusCode() >= 500
            })
    httpClient.JSONUnmarshal = Unmarshal

	service := AquiferService{
		httpClient: httpClient,
		baseUrl: baseUrl,
		workerId: workerId,
		deploymentName: deploymentName,
		deploymentToken: deploymentToken,
		tokenCache: ttlcache.New[string, string](
			ttlcache.WithTTL[string, string](time.Hour * 24 * 7), // 7 days
			ttlcache.WithCapacity[string, string](1000),
		),
	}
	return &service
}

func (service *AquiferService) worker(ctx context.Context,
									  events <-chan AquiferEvent,
									  doneChan chan<- bool) {
	for {
        select {
        case <-ctx.Done():
            return
        case event := <-events:
            if ctx.Err() != nil {
                return
            }
            service.RunJob(ctx, doneChan, event)
        }
    }
}

func (service *AquiferService) StartService() {
	log.Info().Msgf("Starting service: %s", service.deploymentName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
        log.Info().Msgf("%s - Stopping service", sig)
        cancel()
	}()

	go service.tokenCache.Start()

	numWorkers := runtime.NumCPU()
	numWorkers = 1
	var numRunning uint32 = 0

    eventsChan := make(chan AquiferEvent, numWorkers)
    doneChan := make(chan bool, numWorkers)

    // event, _ := parseEvent(
    // 	make(Dict).
    // 		SetString("id", "c06607e1-b899-4816-8d30-d3a716beffa4").
    // 		SetString("type", "file").
    // 		SetString("account_id", "4ec3ba1a-7267-49de-8fa4-ccedaf8804b1").
    // 		SetString("timestamp", "2006-01-02T15:04:05.000000").
    // 		Set("source", make(Dict).
    // 			SetString("type", "router").
    // 			SetString("flow_id", "e40e8bae-52c3-4c1c-8a9c-5e5ed59209dd").
    // 			SetString("job_type", "file").
    // 			SetString("job_id", "c06607e1-b899-4816-8d30-d3a716beffa4")).
    // 		Set("destination", make(Dict).
    // 			SetString("type", "processor").
    // 			SetString("id", "7081616d-f82c-4ab1-9622-1c4c58bcab81").
    // 			SetString("flow_id", "e40e8bae-52c3-4c1c-8a9c-5e5ed59209dd").
    // 			SetString("job_type", "file").
    // 			SetString("job_id", "c06607e1-b899-4816-8d30-d3a716beffa4")).
    // 		Set("payload", make(Dict)))

    // event, _ := parseEvent(
    // 	make(Dict).
    // 		SetString("id", "c06607e1-b899-4816-8d30-d3a716beffa4").
    // 		SetString("type", "extract").
    // 		SetString("account_id", "4ec3ba1a-7267-49de-8fa4-ccedaf8804b1").
    // 		SetString("timestamp", "2006-01-02T15:04:05.000000").
    // 		Set("source", make(Dict).
    // 			SetString("type", "scheduler").
    // 			SetString("flow_id", "d7b17483-b379-4244-ab78-2b279ca37d79")).
    // 		Set("destination", make(Dict).
    // 			SetString("type", "integration").
    // 			SetString("id", "f61a89a0-a4b7-4f6f-988c-5963af568f0b").
    // 			SetString("flow_id", "d7b17483-b379-4244-ab78-2b279ca37d79").
    // 			SetString("job_type", "extract").
    // 			SetString("job_id", "6bb9ff71-ccc1-46ed-a6ad-1b5d71435f8c")).
    // 		Set("payload", make(Dict)))

    // service.RunJob(ctx, doneChan, event)

    // return

    log.Info().Msgf("Creating workers: %d", numWorkers)

    // TODO: what if a worker dies?
    for w := 1; w <= numWorkers; w++ {
        go service.worker(ctx, eventsChan, doneChan)
    }

	for {
		drained := false
		for !drained {
			select {
			case <-ctx.Done():
            	return
		    case <-doneChan:
		    	if ctx.Err() != nil {
	                return
	            }
		        numRunning -= 1
		    default:
		        drained = true
		    }
		}

		maxEvents := uint32(numWorkers) - numRunning
		if maxEvents > 0 {
			log.Info().Msgf("Getting events - maxEvents: %d", maxEvents)
			events, err := service.getEvents(ctx, int(maxEvents))
			if err != nil && ctx.Err() == nil {
				log.Error().
					Stack().
					Err(err).
					Msg("Error fetching events - sleeping for 2 seconds")
				time.Sleep(2 * time.Second)
				continue // TODO: force exit?
			}

			for _, event := range events {
				numRunning += 1
				eventsChan <- event
			}
		} else {
			select {
	        case <-ctx.Done():
	            return
	        case <-doneChan: // TODO: on sigterm wait until numRunning is 0
	            if ctx.Err() != nil {
	                return
	            }
	            numRunning -= 1
	        }
		}
	}
}

func (service *AquiferService) RunJob(ctx context.Context, doneChan chan<- bool, event AquiferEvent) {
	defer func() {
		doneChan <- true
	}()

	var (
		job JobInterface
		err error
	)

	defer func() {
		r := recover()
		if r != nil && err == nil {
			err = r.(error)
			log.Error().
				Err(err).
				Str("account_id", event.AccountId.String()).
				Str("job_type", event.Destination.JobType).
				Str("job_id", event.Destination.JobId.String()).
				Str("flow_id", event.Destination.FlowId.String()).
				Str("entity_type", event.Destination.Type).
				Str("entity_id", event.Destination.Id.String()).
				Msgf("panic: %s", r)
		}

		if job != nil {
			releaseStatus := "release"
			var failureErrorId uuid.UUID
			if err != nil {
				job.Logger().
					Error().
					Stack().
					Err(err).
					Msg("Error running job")
				// TODO: report error
				releaseStatus = "failed"
			} else if job.IsTimedout() == true {
				job.Logger().
					Error().
					Bool("timeout", true).
					Msg("Error running job - timedout")
				// TODO: report error
				releaseStatus = "failed"
			} else if job.GetCtx().Err() == nil {
				releaseStatus = "success"
			}

			err = job.Release(releaseStatus, failureErrorId)
			if err != nil {
				job.Logger().
					Error().
					Stack().
					Err(err).
					Str("release_status", releaseStatus).
					Msg("Error releasing job")
				// TODO: report error
			} else {
				job.Logger().
					Info().
					Str("release_status", releaseStatus).
					Msgf("Job finished: %s", job.GetType())
			}
		}
	}()

	job, err = NewAquiferJobFromEvent(service, ctx, event)
	if err != nil {
		log.Error().
			Err(err).
			Str("account_id", event.AccountId.String()).
			Str("job_type", event.Destination.JobType).
			Str("job_id", event.Destination.JobId.String()).
			Str("flow_id", event.Destination.FlowId.String()).
			Str("entity_type", event.Destination.Type).
			Str("entity_id", event.Destination.Id.String()).
			Msg("Error starting job")
		return
	}

	err = job.Lock()

	if err == nil {
		jobType := job.GetType()

		job.Logger().Info().Msgf("Running job: %s", jobType)

		if jobType == "extract" {
			err = service.extractHandler(job)
		} else if jobType == "data-batch" {
			err = service.dataHandler(job)
		} else if jobType == "file" {
			err = service.fileHandler(job)
		} else {
			err = fmt.Errorf("Job type not supported: %s", jobType)
		}
	}
}

func (service *AquiferService) SetFileHandler(fn func(JobInterface) error) {
	service.fileHandler = fn
}

func (service *AquiferService) SetDataHandler(fn func(JobInterface) error) {
	service.dataHandler = fn
}

func (service *AquiferService) SetExtractHandler(fn func(JobInterface) error) {
	service.extractHandler = fn
}

func (service *AquiferService) Request(ctx context.Context,
									   method string,
	                                   path string,
	                                   body interface{},
	                                   token string) (data Dict, err error) {
    if token == "" {
    	token = service.deploymentToken
    }

    url := service.baseUrl + path

    req := service.httpClient.R().
		SetHeader("Accept", "application/json").
		SetAuthToken(token).
		SetContext(ctx).
		SetResult(&data)

	if body != nil {
		req = req.SetBody(body)
	}

	var resp *resty.Response
	resp, err = req.Execute(method, url)

	if resp.StatusCode() > 299 {
		err = fmt.Errorf("Non-200 status code: %d %s",
						 resp.StatusCode(),
						 url)
	}

    return
}

func (service *AquiferService) GetEntityToken(ctx context.Context,
											  accountId uuid.UUID,
											  entityType string,
											  entityId uuid.UUID) (token string, err error) {
	cacheKey := fmt.Sprintf("%s%s%s", accountId, entityType, entityId)
	item := service.tokenCache.Get(cacheKey)
	if item != nil {
		token = item.Value()
		return
	}

	var entityPath string
	entityPath, err = service.GetEntityPath(accountId, entityType, entityId)
	if err != nil {
		return
	}

	var data Dict
	data, err = service.Request(
		ctx,
		"POST",
		fmt.Sprintf("%s/token", entityPath),
		nil,
	    "")
	if err != nil {
		return
	}

	token = data.Get("data").Get("attributes").GetString("deployment_entity_token")

	service.tokenCache.Set(cacheKey, token, ttlcache.DefaultTTL)

	return
}

func (service *AquiferService) GetEntityPath(accountId uuid.UUID,
											 entityType string,
											 entityId uuid.UUID) (entityPath string, err error) {
	var pathType string
	pathType, err = getEntityPathType(entityType)
	if err != nil {
		return
	}
	entityPath = fmt.Sprintf("/accounts/%s/%s/%s",
		accountId,
		pathType,
		entityId)
	return
}

func (service *AquiferService) GetCli() *cli.App {
    app := &cli.App{
        Flags: []cli.Flag{
            &cli.StringFlag{
                Name: "base-url",
                Usage: "Aquifer API base URL",
                Value: DEFAULT_BASE_URL,
                // EnvVars: "AQUIFER_BASE_URL",
            },
            &cli.StringFlag{
                Name: "token",
                Usage: "Aquifer deployment token",
                // EnvVars: "AQUIFER_TOKEN",
            },
            &cli.StringFlag{
                Name: "worker-id",
                Usage: "Aquifer service worker ID",
                // Value: fmt.Sprintf("%s-0", service.deploymentName),
                // EnvVars: "AQUIFER_WORKER_ID",
            },
        },
        Commands: []*cli.Command{
            {
                Name: "run-service",
                Usage: "run as Aquifer service",
                Action: func(cCtx *cli.Context) error {
                    service.StartService()
                    return nil
                },
            },
            {
                Name: "run-job",
                Usage: "run an Aquifer job",
                Flags: []cli.Flag{
                    &cli.StringFlag{
                        Name: "account-id",
                        Usage: "Account ID for JOB",
                        Required: true,
                    },
                    &cli.StringFlag{
                        Name: "flow-id",
                        Usage: "Flow ID for JOB",
                        Required: true,
                    },
                    &cli.StringFlag{
                        Name: "entity-type",
                        Usage: "Entity type for JOB - integration, datastore, processor, or blobstore",
                        Required: true,
                    },
                    &cli.StringFlag{
                        Name: "entity-id",
                        Usage: "Entity ID for JOB",
                        Required: true,
                    },
                    &cli.StringFlag{ // TODO: use separate by-id command?
                        Name: "job-id",
                        Usage: "Run specific Job by ID",
                    },
                    &cli.BoolFlag{
                        Name: "dry-run",
                        Usage: "Entity ID for JOB",
                    },
                },
                Subcommands: []*cli.Command{
                    {
                        Name:  "schema-sync",
                        Usage: "schema sync job",
                        Action: func(cCtx *cli.Context) error {
                            fmt.Println("new task template: ", cCtx.Args().First())
                            return nil
                        },
                    },
                    {
                        Name:  "extract",
                        Usage: "extract job",
                        Action: func(cCtx *cli.Context) error {
                            fmt.Println("new task template: ", cCtx.Args().First())
                            return nil
                        },
                    },
                },
            },
        },
    }
    return app
}

func (service *AquiferService) RunCli() {
    app := service.GetCli()
    if err := app.Run(os.Args); err != nil {
        log.Fatal().Err(err).Msg("Fatal error")
    }
}

func (service *AquiferService) getEvents(ctx context.Context, maxEvents int) (events []AquiferEvent, err error) {
	// resty will do retries, idempotent_id makes sure we treat this as one message fetch
	idempotentId := uuid.New().String()

	var data Dict
	data, err = service.Request(
		ctx,
		"GET",
		fmt.Sprintf(
			"/deployments/%s/events?max_messages=%d&idempotent_id=%s",
			service.deploymentName,
			maxEvents,
			idempotentId),
		nil,
	    "")
	if err != nil {
		return
	}

	rawEvents := data.Get("data").Get("attributes").GetArray("events")

	for _, rawEvent := range rawEvents {
		var event AquiferEvent
		event, err = parseEvent(Dict(rawEvent.(map[string]interface{})))
		if err != nil {
			return
		}
		events = append(events, event)
	}
	return
}

func parseEvent(rawEvent Dict) (event AquiferEvent, err error) {
	var eventId uuid.UUID
	eventId, err = uuid.Parse(rawEvent.GetString("id"))
	if err != nil {
		return
	}

	var accountId uuid.UUID
	accountId, err = uuid.Parse(rawEvent.GetString("account_id"))
	if err != nil {
		return
	}

	var timestamp time.Time
	layout := "2006-01-02T15:04:05.000000"
	timestamp, err = time.Parse(layout, rawEvent.GetString("timestamp"))
	if err != nil {
		return
	}

	source := rawEvent.Get("source")
	var sourceId uuid.UUID
	sourceId, err = uuid.Parse(source.GetString("id"))
	var sourceFlowId uuid.UUID
	sourceFlowId, err = uuid.Parse(source.GetString("flow_id"))
	var sourceJobId uuid.UUID
	sourceJobId, err = uuid.Parse(source.GetString("job_id"))

	destination := rawEvent.Get("destination")
	var destinationId uuid.UUID
	destinationId, err = uuid.Parse(destination.GetString("id"))
	var destinationFlowId uuid.UUID
	destinationFlowId, err = uuid.Parse(destination.GetString("flow_id"))
	var destinationJobId uuid.UUID
	destinationJobId, err = uuid.Parse(destination.GetString("job_id"))

	event = AquiferEvent{
		Id: eventId,
		Type: rawEvent.GetString("type"),
		AccountId: accountId,
		Timestamp: timestamp,
		Source: EventSource{
			Type: source.GetString("type"),
			Id: sourceId,
			FlowId: sourceFlowId,
			JobType: source.GetString("job_type"),
			JobId: sourceJobId,
		},
		Destination: EventDestination{
			Type: destination.GetString("type"),
			Id: destinationId,
			FlowId: destinationFlowId,
			JobType: destination.GetString("job_type"),
			JobId: destinationJobId,
			Handle: destination.GetString("handle"),
		},
		Payload: rawEvent.Get("payload"),
	}
	return
}

func getEntityPathType(entityType string) (pathType string, err error) {
	if entityType == "integration" {
		pathType = "integrations"
	} else if entityType == "processor" {
		pathType = "processors"
	} else if entityType == "blobstore" {
		pathType = "blobstores"
	} else if entityType == "datastore" {
		pathType = "datastores"
	} else {
		err = fmt.Errorf("Entity type has no path type: %s", entityType)
	}
	return
}
