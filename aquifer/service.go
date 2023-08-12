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
    isAccountDeployment bool
	tokenCache *ttlcache.Cache[string, string]
    connectionTestHandler func(JobInterface) error
	fileHandler func(JobInterface) error
	dataHandler func(JobInterface) error
    queryHandler func(JobInterface) error
    snapshotCompleteHandler func(JobInterface) error
    hyperbatchCompleteHandler func(JobInterface) error
	extractHandler func(JobInterface) error
    schemaSyncHandler func(JobInterface) error
}

type ServiceOptions struct {
    DeploymentName *string
    DeploymentToken *string
    IsAccountDeployment *bool
    BaseUrl *string
    WorkerId *string
    ApiRetriesMax *int
    ApiRetriesWaitTimeSec *int
    ApiRetriesMaxWaitTimeSec *int
}

func NewService(options *ServiceOptions) *AquiferService {
    zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

    var baseUrl string
    if options.BaseUrl == nil {
        baseUrl = getenv("AQUIFER_BASE_URL", DEFAULT_BASE_URL)
    } else {
        baseUrl = *options.BaseUrl
    }

    var deploymentName string
    if options.DeploymentName != nil {
        deploymentName = *options.DeploymentName
    }

	var deploymentToken string
    if options.DeploymentToken == nil {
        deploymentToken = os.Getenv("AQUIFER_TOKEN")
    } else {
        deploymentToken = *options.DeploymentToken
    }

    var isAccountDeployment bool
    if options.IsAccountDeployment == nil {
        isAccountDeployment = os.Getenv("AQUIFER_ACCOUNT_DEPLOYMENT") == "true"
    } else {
        isAccountDeployment = *options.IsAccountDeployment
    }

	var workerId string
    if options.WorkerId == nil {
        workerId = getenv("AQUIFER_WORKER_ID",
					      fmt.Sprintf("%s-0", deploymentName))
    } else {
        workerId = *options.WorkerId
    }

    var apiRetriesMax int
    if options.ApiRetriesMax == nil {
        apiRetriesMax = 5
    } else {
        apiRetriesMax = *options.ApiRetriesMax
    }

    var apiRetriesWaitTime time.Duration
    if options.ApiRetriesWaitTimeSec == nil {
        apiRetriesWaitTime = 5
    } else {
        apiRetriesWaitTime = time.Duration(*options.ApiRetriesWaitTimeSec)
    }

    var apiRetriesMaxWaitTime time.Duration
    if options.ApiRetriesMaxWaitTimeSec == nil {
        apiRetriesMaxWaitTime = 30
    } else {
        apiRetriesMaxWaitTime = time.Duration(*options.ApiRetriesMaxWaitTimeSec)
    }

    httpClient := resty.New().
        SetRetryCount(apiRetriesMax).
        SetRetryWaitTime(apiRetriesWaitTime * time.Second).
        SetRetryMaxWaitTime(apiRetriesMaxWaitTime * time.Second).
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
        isAccountDeployment: isAccountDeployment,
		tokenCache: ttlcache.New[string, string](
			ttlcache.WithTTL[string, string](time.Hour * 24 * 7), // 7 days
            ttlcache.WithDisableTouchOnHit[string, string](),
			ttlcache.WithCapacity[string, string](1000),
		),
	}
	return &service
}

func (service *AquiferService) SetIsAccountDeployment(isAccountDeployment bool) {
    service.isAccountDeployment = isAccountDeployment
}

func (service *AquiferService) GetIsAccountDeployment() bool {
    return service.isAccountDeployment
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
	var numRunning uint32 = 0

    eventsChan := make(chan AquiferEvent, numWorkers)
    doneChan := make(chan bool, numWorkers)

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
                Stack().
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
			var failureErrorId *uuid.UUID
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
        } else if jobType == "schema-sync" {
            err = service.schemaSyncHandler(job)
		} else if jobType == "data-batch" {
			err = service.dataHandler(job)
        } else if jobType == "query" {
            err = service.queryHandler(job)
		} else if jobType == "file" {
			err = service.fileHandler(job)
        } else if jobType == "snapshot" {
            err = service.snapshotCompleteHandler(job)
		} else {
			err = fmt.Errorf("Job type not supported: %s", jobType)
		}
	}
}

func (service *AquiferService) SetConnectionTestHandler(fn func(JobInterface) error) {
    service.connectionTestHandler = fn
}

func (service *AquiferService) SetFileHandler(fn func(JobInterface) error) {
	service.fileHandler = fn
}

func (service *AquiferService) SetDataHandler(fn func(JobInterface) error) {
	service.dataHandler = fn
}

func (service *AquiferService) SetQueryHandler(fn func(JobInterface) error) {
    service.queryHandler = fn
}

func (service *AquiferService) SetSnapshotCompleteHandler(fn func(JobInterface) error) {
    service.snapshotCompleteHandler = fn
}

func (service *AquiferService) SetHyperbatchCompleteHandler(fn func(JobInterface) error) {
    service.hyperbatchCompleteHandler = fn
}

func (service *AquiferService) SetExtractHandler(fn func(JobInterface) error) {
	service.extractHandler = fn
}

func (service *AquiferService) SetSchemaSyncHandler(fn func(JobInterface) error) {
    service.schemaSyncHandler = fn
}

type RequestOptions struct {
    Token string
    Body interface{}
    Ignore404 bool
}

func (service *AquiferService) Request(ctx context.Context,
									   method string,
	                                   path string,
	                                   options RequestOptions) (data Dict, err error) {
    var token string
    if options.Token != "" {
        token = options.Token
    } else {
    	token = service.deploymentToken
    }

    url := service.baseUrl + path

    req := service.httpClient.R().
		SetHeader("Accept", "application/json").
		SetAuthToken(token).
		SetContext(ctx).
		SetResult(&data)

	if options.Body != nil {
		req = req.SetBody(options.Body)
	}

	var resp *resty.Response
	resp, err = req.Execute(method, url)

    if options.Ignore404 && resp.StatusCode() == 404 {
        return
    }

	if resp.StatusCode() > 299 {
		err = fmt.Errorf("Non-200 status code: %d %s %s",
						 resp.StatusCode(),
                         method,
						 url)
	}

    return
}

func (service *AquiferService) GetEntityToken(ctx context.Context,
											  accountId uuid.UUID,
											  entityType string,
											  entityId *uuid.UUID) (token string, err error) {
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
		RequestOptions{})
	if err != nil {
		return
	}

	token = data.Get("data").Get("attributes").GetString("deployment_entity_token")

	service.tokenCache.Set(cacheKey, token, ttlcache.DefaultTTL)

	return
}

func (service *AquiferService) GetEntityPath(accountId uuid.UUID,
											 entityType string,
											 entityId *uuid.UUID) (entityPath string, err error) {
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
    dryRunFlag := &cli.BoolFlag{
        Name: "dry-run",
        Usage: "Dry Run - Do not perform write operations",
    }

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
            &cli.BoolFlag{
                Name: "account-deployment",
                Usage: "Account specific Aquifer deployment",
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
                Name: "run-job-by-id",
                Usage: "run an Aquifer job",
                Flags: []cli.Flag{
                    &cli.StringFlag{
                        Name: "account-id",
                        Usage: "Account ID for Job",
                        Required: true,
                    },
                    &cli.StringFlag{
                        Name: "job-type",
                        Usage: "Job Type",
                        Required: true,
                    },
                    &cli.StringFlag{
                        Name: "job-id",
                        Usage: "Job ID",
                        Required: true,
                    },
                    dryRunFlag,
                },
                Action: func(cCtx *cli.Context) error {
                    return nil
                },
            },
            {
                Name: "run-job",
                Usage: "run an Aquifer job",
                Subcommands: []*cli.Command{
                    {
                        Name: "schema-sync",
                        Usage: "Run a schema sync job",
                        Flags: []cli.Flag{
                            &cli.StringFlag{
                                Name: "account-id",
                                Usage: "Account ID for Job",
                                Required: true,
                            },
                            &cli.StringFlag{
                                Name: "entity-type",
                                Usage: "Entity type for Job - integration, datastore, processor, or blobstore",
                                Required: true,
                            },
                            &cli.StringFlag{
                                Name: "entity-id",
                                Usage: "Entity ID for Job",
                                Required: true,
                            },
                            dryRunFlag,
                        },
                        Action: func(cCtx *cli.Context) (err error) {
                            jobCtx, jobCancel := context.WithCancel(context.Background())

                            jobType := "schema-sync"
                            accountIdStr := cCtx.String("account-id")
                            entityType := cCtx.String("entity-type")
                            entityIdStr := cCtx.String("entity-id")
                            accountDeployment := cCtx.Bool("account-deployment")

                            if accountDeployment {
                                service.SetIsAccountDeployment(accountDeployment)
                            }

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

                            jobCtx = log.With().
                                Str("account_id", accountIdStr).
                                Str("job_type", jobType).
                                Str("entity_type", entityType).
                                Str("entity_id", entityIdStr).
                                Logger().
                                WithContext(jobCtx)
                            logger := log.Ctx(jobCtx)

                            job := &AquiferJob{
                                service: service,
                                ctx: jobCtx,
                                logger: logger,
                                cancel: jobCancel,
                                accountId: accountId,
                                entityType: entityType,
                                entityId: &entityId,
                                jobType: jobType,
                            }

                            return service.schemaSyncHandler(job)
                        },
                    },
                    {
                        Name: "extract",
                        Usage: "Run an extract job",
                        Flags: []cli.Flag{
                            &cli.StringFlag{
                                Name: "account-id",
                                Usage: "Account ID for Job",
                                Required: true,
                            },
                            &cli.StringFlag{
                                Name: "entity-type",
                                Usage: "Entity type for Job - integration, datastore, processor, or blobstore",
                                Required: true,
                            },
                            &cli.StringFlag{
                                Name: "entity-id",
                                Usage: "Entity ID for Job",
                                Required: true,
                            },
                            &cli.StringFlag{
                                Name: "flow-id",
                                Usage: "Flow ID for Job",
                                Required: true,
                            },
                            dryRunFlag,
                        },
                        Action: func(cCtx *cli.Context) error {
                            accountIdStr := cCtx.String("account-id")
                            entityType := cCtx.String("entity-type")
                            entityIdStr := cCtx.String("entity-id")
                            flowIdStr := cCtx.String("flow-id")
                            accountDeployment := cCtx.Bool("account-deployment")

                            if accountDeployment {
                                service.SetIsAccountDeployment(accountDeployment)
                            }

                            job, err := NewJobFromCLI(
                                service,
                                context.Background(),
                                "extract",
                                accountIdStr,
                                flowIdStr,
                                entityType,
                                entityIdStr)
                            if err != nil {
                                return err
                            }
                            err = job.Lock()
                            if err != nil {
                                return err
                            }

                            return service.extractHandler(job)
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
        fmt.Println(err)
    }
}

type JSONAPIEvents struct {
    Events []AquiferEvent `json:"events"`
}

type JSONAPIData struct {
    Id string `json:"id"`
    Attributes JSONAPIEvents `json:"attributes"`
}

type JSONAPIResponse struct {
    Data JSONAPIData `json:"data"`
}

func (service *AquiferService) getEvents(ctx context.Context, maxEvents int) (events []AquiferEvent, err error) {
	// resty will do retries, idempotent_id makes sure we treat this as one message fetch
	idempotentId := uuid.New().String()

	var data JSONAPIResponse
    url := service.baseUrl + fmt.Sprintf(
         "/deployments/%s/events?max_messages=%d&idempotent_id=%s",
         service.deploymentName,
         maxEvents,
         idempotentId)

    req := service.httpClient.R().
        SetHeader("Accept", "application/json").
        SetAuthToken(service.deploymentToken).
        SetContext(ctx).
        SetResult(&data)

    var resp *resty.Response
    resp, err = req.Execute("GET", url)

    if resp.StatusCode() > 299 {
        err = fmt.Errorf("Non-200 status code: %d %s",
                         resp.StatusCode(),
                         url)
    }

    events = data.Data.Attributes.Events
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
