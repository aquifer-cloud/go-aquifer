package aquifer

import (
    "io"
    "fmt"
    "time"
    "sync"
    "bytes"
    "context"
    "net/http"
	"compress/gzip"

 	"github.com/google/uuid"
 	"github.com/rs/zerolog/log"
 	"github.com/go-resty/resty/v2"
)

type AquiferFile struct {
    service *AquiferService
    ctx context.Context
    closed bool
    jobType string
    httpClient http.Client
    accountId uuid.UUID
	entityType string
	entityId *uuid.UUID
    fileId *uuid.UUID
    globalId *uuid.UUID
    mode string
    compression string
    gzipEnabled bool
    gzipWriter *gzip.Writer
    gzipReader *gzip.Reader
    gzipLocation int
    downloadUrl string
    downloadUrlLock sync.Mutex
    expiresAt time.Time
    uploadToken string
    uploadParts []Dict
    partNumber int
    buffer bytes.Buffer
    bufStart int
    bufEnd int
    sizeBytes int
    sizeBytesUncompressed int
    location int
    chunkSize int
    attributesFn func(*AquiferFile) (Dict, error)
}

type AquiferFileInterface interface {
	io.Reader
	io.ReaderAt
	io.Closer
	Init() error
	GetId() *uuid.UUID
	GetChunkSize() int
	GetSize() int
}

func NewAquiferFile(service *AquiferService,
					ctx context.Context,
					mode string,
					gzipEnabled bool,
					jobType string,
					accountId uuid.UUID,
					entityType string,
					entityId *uuid.UUID,
					fileId *uuid.UUID) *AquiferFile {
    file := AquiferFile{
    	service: service,
    	ctx: ctx,
    	jobType: jobType,
    	httpClient: http.Client{},
    	accountId: accountId,
    	entityType: entityType,
    	entityId: entityId,
    	fileId: fileId,
    	mode: mode,
    	gzipEnabled: gzipEnabled,
        uploadParts: make([]Dict, 0),
        location: 0,
        chunkSize: 1024 * 1024 * 64, // 64MB
    }
    return &file
}

func (file *AquiferFile) GetId() *uuid.UUID {
	return file.fileId
}

func (file *AquiferFile) GetGlobalId() *uuid.UUID {
    return file.globalId
}

func (file *AquiferFile) SetGlobalId(globalId uuid.UUID) {
    file.globalId = &globalId
}

func (file *AquiferFile) Init() error {
	if file.mode[0] == 'r' {
		return file.ensureDownloadUrl(file.ctx)
	}
	return nil
}

func (file *AquiferFile) SetAttributesFn(attributesFn func (file *AquiferFile) (attributes Dict, err error)) {
	file.attributesFn = attributesFn
}

func (file *AquiferFile) GetSize() int {
	if file.mode[0] == 'w' {
		return file.sizeBytes + file.buffer.Len()
	} else {
		return file.sizeBytes
	}
}

func (file *AquiferFile) GetChunkSize() int {
	return file.chunkSize
}

func (file *AquiferFile) Tell() int {
	return file.location
}

func (file *AquiferFile) SetGzipEnabled(gzipEnabled bool) {
	file.gzipEnabled = gzipEnabled
}

func (file *AquiferFile) DirectReadAt(start int, end int) (chunk []byte, err error) {
	err = file.ensureDownloadUrl(file.ctx)
	if err != nil {
		return
	}

	var resp *resty.Response
    resp, err = file.service.httpClient.R().
    	SetHeader("Range", fmt.Sprintf("bytes=%d-%d", start, end)).
    	SetContext(file.ctx).
		Get(file.downloadUrl)

    if resp.StatusCode() == 416 {
    	err = io.EOF
    	return
    }

    if resp.StatusCode() > 299 {
    	err = fmt.Errorf("Non-200 response from file download: %d", resp.StatusCode())
    	return
    }

    chunk = resp.Body()
    return
}

func (file *AquiferFile) ReadAt(p []byte, off int64) (n int, err error) {
	if file.gzipEnabled && file.compression == "gzip" {
		err = fmt.Errorf("ReadAt not supported for gzip files")
		return
	}

	// call outside of DirectRead to ensure file.sizeBytes is populated
	err = file.ensureDownloadUrl(file.ctx)
	if err != nil {
		return
	}

	offInt := int(off)

	if offInt >= file.sizeBytes {
		err = io.EOF
		return
	}

	// TODO: buffering? can't use bytes.Buffer

	var chunk []byte
	chunk, err = file.DirectReadAt(offInt, offInt + len(p))
	if err != nil {
		return
	}

	n = copy(p, chunk)

	return
}

func (file *AquiferFile) Read(p []byte) (n int, err error) {
	if err = file.ctx.Err(); err != nil {
		return
	}

	// call outside of DirectRead to ensure file.sizeBytes is populated
	err = file.ensureDownloadUrl(file.ctx)
	if err != nil {
		return
	}

	attempts := 1
    for file.buffer.Len() < len(p) && file.location < (file.sizeBytes - 1) {
        if attempts > 5 {
        	log.Error().
        		Str("job_id", file.fileId.String()).
        		Int("buffer_len", file.buffer.Len()).
        		Int("p_len", len(p)).
        		Int("file_location", file.location).
        		Int("file_size_bytes", file.sizeBytes).
        		Msg("Too many attempts to read from S3")
        	err = fmt.Errorf("Too many attempts to read from S3")
        	return
        }

        start := file.location
        end := (start + file.chunkSize) - 1

        var chunk []byte
        chunk, err = file.DirectReadAt(start, end)

        file.location += len(chunk)
        file.buffer.Write(chunk)

        attempts += 1
    }

    if file.gzipEnabled && file.compression == "gzip" {
		if file.gzipReader == nil {
			file.gzipReader, err = gzip.NewReader(&file.buffer)
			if err != nil {
				return
			}
		}

		n, err = file.gzipReader.Read(p)
		if n > 0 && err == io.EOF {
			err = nil
		}
	} else {
		if file.buffer.Len() == 0 && file.location == file.sizeBytes {
			err = io.EOF
			return
		}

		n = copy(p, file.buffer.Next(len(p)))
	}

    return
}

func (file *AquiferFile) ensureDownloadUrl(ctx context.Context) (err error) {
	file.downloadUrlLock.Lock()
	defer file.downloadUrlLock.Unlock()

	if file.downloadUrl == "" || file.expiresAt.Before(time.Now()) {
		var token string
		token, err = file.service.GetEntityToken(ctx, file.accountId, file.entityType, file.entityId)
		if err != nil {
			return
		}

		var filePath string
		if file.globalId != nil {
			filePath = fmt.Sprintf(
				"/accounts/%s/files/download/%s",
				file.accountId,
				file.globalId)
		} else {
			var jobPath string
			if file.jobType == "file" {
				jobPath = "files"
			} else {
				jobPath = "data"
			}

			filePath = fmt.Sprintf(
				"/accounts/%s/%s/%s",
				file.accountId,
				jobPath,
				file.fileId)
		}

		var data Dict
		data, err = file.service.Request(
			ctx,
			"GET",
			filePath,
			RequestOptions{
				Token: token,
			})
		if err != nil {
			return
		}

		attributes := data.Get("data").Get("attributes")
		file.downloadUrl = attributes.GetString("download_url")
		// expire in at hour with 30 seconds of padding
		file.expiresAt = time.Now().Add(time.Second * 3570)
		file.compression = attributes.GetString("compression")

		file.sizeBytes, _ = attributes.GetInt("size_bytes")
	}
	return
}

func (file *AquiferFile) Write(chunk []byte) (n int, err error) {
	if err = file.ctx.Err(); err != nil {
		return
	}

	if file.gzipEnabled {
		if file.gzipWriter == nil {
			file.gzipWriter = gzip.NewWriter(&file.buffer)
		}

		var uncN int
		uncN, err = file.gzipWriter.Write(chunk)
		file.sizeBytesUncompressed += uncN
	} else {
		_, err = file.buffer.Write(chunk)
	}

	if err != nil {
		return
	}

	n = len(chunk)

	if file.buffer.Len() >= file.chunkSize {
		file.Flush(false)
	}

	return
}

func (file *AquiferFile) Close() (err error) {
	if !file.closed {
		err = file.Flush(true)
		if err != nil {
			return
		}

		err = file.uploadComplete()

		file.closed = true
	}

	return
}

func (file *AquiferFile) Cancel() error {
	return file.uploadCancel()
}

func (file *AquiferFile) Flush(full bool) (err error) {
	if file.gzipEnabled && full {
		err = file.gzipWriter.Close()
		if err != nil {
			return
		}
	}

	if file.uploadToken == "" {
		err = file.uploadInit()
		if err != nil {
			return
		}
	}
	for file.buffer.Len() >= file.chunkSize ||
	    (full && file.buffer.Len() > 0) {
	    uploadChunk := file.buffer.Next(file.chunkSize)
	    err = file.uploadPart(uploadChunk)
	    if err != nil {
	    	return
	    }
	    file.sizeBytes += len(uploadChunk)
	}
	return
}

func (file *AquiferFile) uploadInit() (err error) {
	var token string
	token, err = file.service.GetEntityToken(file.ctx, file.accountId, file.entityType, file.entityId)
	if err != nil {
		return
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", file.getApiType()).
			Set("attributes", make(Dict)))

	var data Dict
	data, err = file.service.Request(
		file.ctx,
		"POST",
		fmt.Sprintf("/accounts/%s/%s/upload",
			file.accountId,
			file.getPathType()),
		RequestOptions{
			Token: token,
			Body: reqData,
		})
	if err != nil {
		return
	}

	rawId := data.Get("data").GetString("id")
	var fileId uuid.UUID
	fileId, err = uuid.Parse(rawId)
	if err != nil {
		return
	}
	file.fileId = &fileId

	file.uploadToken = data.Get("data").Get("attributes").GetString("upload_token")
	file.partNumber, _ = data.Get("data").Get("attributes").GetInt("part_number")

	return
}

func (file *AquiferFile) uploadPart(chunk []byte) (err error) {
	var token string
	token, err = file.service.GetEntityToken(file.ctx, file.accountId, file.entityType, file.entityId)
	if err != nil {
		return
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", file.getApiType()).
			Set("attributes", make(Dict).
				SetString("upload_token", file.uploadToken).
				SetInt("part_number", file.partNumber)))

	var data Dict
	data, err = file.service.Request(
		file.ctx,
		"POST",
		fmt.Sprintf("/accounts/%s/%s/upload",
			file.accountId,
			file.getPathType()),
		RequestOptions{
			Token: token,
			Body: reqData,
		})
	if err != nil {
		return
	}

	resp, err := file.service.httpClient.R().
		SetBody(chunk).
		SetContext(file.ctx).
		Put(data.Get("data").Get("attributes").GetString("upload_url"))
    if err != nil {
        return
    }
    if resp.StatusCode() > 299 {
    	err = fmt.Errorf(
    		"Non-200 response uploading part: %d - %s - %d",
    		resp.StatusCode(),
    		file.fileId.String(),
    		file.partNumber)
    	return
    }

    file.uploadParts = append(
    	file.uploadParts,
    	make(Dict).
    		SetInt("part_number", file.partNumber).
    		SetString("etag", resp.Header()["Etag"][0]))

    file.partNumber += 1

    return
}

func (file *AquiferFile) uploadComplete() (err error) {
	var token string
	token, err = file.service.GetEntityToken(file.ctx, file.accountId, file.entityType, file.entityId)
	if err != nil {
		return
	}

	var attributes Dict
	attributes, err = file.attributesFn(file)
	if err != nil {
		return
	}

	if file.gzipEnabled {
		attributes = attributes.
			SetString("compression", "gzip").
			SetInt("size_bytes_uncompressed", file.sizeBytesUncompressed)
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", file.getApiType()).
			Set("attributes", attributes.
				SetString("upload_token", file.uploadToken).
				SetArray("parts", file.uploadParts)))

	_, err = file.service.Request(
		file.ctx,
		"POST",
		fmt.Sprintf("/accounts/%s/%s/upload/complete",
			file.accountId,
			file.getPathType()),
		RequestOptions{
			Token: token,
			Body: reqData,
		})

	return
}

func (file *AquiferFile) uploadCancel() (err error) {
	var token string
	token, err = file.service.GetEntityToken(file.ctx, file.accountId, file.entityType, file.entityId)
	if err != nil {
		return
	}

	reqData := make(Dict).
		Set("data", make(Dict).
			SetString("type", file.getApiType()).
			Set("attributes", make(Dict).
				SetString("upload_token", file.uploadToken)))

	_, err = file.service.Request(
		file.ctx,
		"DELETE",
		fmt.Sprintf("/accounts/%s/%s/upload/complete",
			file.accountId,
			file.getPathType()),
		RequestOptions{
			Token: token,
			Body: reqData,
		})

	return
}

func (file *AquiferFile) getApiType() (apiType string) {
	if file.jobType == "file" {
		return "file-upload"
	} else if file.jobType == "data-batch" {
		return "data-upload"
	}
	return
}

func (file *AquiferFile) getPathType() (pathType string) {
	if file.jobType == "file" {
		return "files"
	} else if file.jobType == "data-batch" {
		return "data"
	}
	return
}
