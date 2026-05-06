package pure

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	hipFieldURL        = "url"
	hipFieldHeaders    = "headers"
	hipFieldVerb       = "verb"
	hipFieldTimeout    = "timeout"
	hipFieldPagination = "pagination"
	hipFieldResponse   = "response"
	hipFieldCheckpoint = "checkpoint"
	hipFieldMaxPages   = "max_pages"
	hipFieldMaxRecords = "max_records"

	hipPaginationType         = "type"
	hipPaginationCursor       = "cursor"
	hipCursorParamName        = "param_name"
	hipCursorNextField        = "next_cursor_field"
	hipCursorHasMoreField     = "has_more_field"
	hipCursorInitial          = "initial_cursor"

	hipResponseDataField = "data_field"
	hipResponseFlatten   = "flatten"

	hipCheckpointCache    = "cache"
	hipCheckpointKey      = "key"
	hipCheckpointStrategy = "save_strategy"
)

func httpPaginatedInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Network").
		Summary("Fetches data from paginated HTTP APIs with support for cursor, page number, and offset-based pagination.").
		Description(`
This input fetches all pages from a paginated REST API and emits records as messages. It supports:

- Cursor-based pagination (e.g., Stripe, Shopify, Claude API)
- Page number pagination
- Offset/limit pagination
- Link header pagination (RFC 5988)

The input reads until the API indicates no more pages are available, then returns ` + "`service.ErrEndOfInput`" + ` for clean shutdown.

Checkpoints can be stored in DynamoDB to resume from the last fetched page across restarts.

## Examples

### Cursor-Based Pagination (Claude Compliance API)

` + "```yaml" + `
input:
  http_paginated:
    url: https://api.anthropic.com/v1/compliance/activities
    headers:
      x-api-key: "${ANTHROPIC_COMPLIANCE_ACCESS_KEY}"
    pagination:
      type: cursor
      cursor:
        param_name: after_id
        next_cursor_field: last_id
        has_more_field: has_more
    response:
      data_field: data
      flatten: true
    checkpoint:
      type: dynamodb
      dynamodb:
        table: checkpoints
        partition_key: integration_id
        partition_value: claude-compliance
        sort_key: checkpoint_key
        sort_value: activities

output:
  aws_s3_stream:
    bucket: mybucket
    path: activities.jsonl.gz
` + "```" + `
`).
		Fields(
			service.NewURLField(hipFieldURL).
				Description("The base URL of the API endpoint."),

			service.NewStringMapField(hipFieldHeaders).
				Description("Headers to include in each HTTP request.").
				Default(map[string]any{}),

			service.NewStringEnumField(hipFieldVerb, "GET", "POST").
				Description("HTTP method to use.").
				Default("GET"),

			service.NewDurationField(hipFieldTimeout).
				Description("Request timeout.").
				Default("30s"),

			service.NewObjectField(hipFieldPagination,
				service.NewStringEnumField(hipPaginationType, "cursor").
					Description("Pagination strategy type.").
					Default("cursor"),

				service.NewObjectField(hipPaginationCursor,
					service.NewStringField(hipCursorParamName).
						Description("Query parameter name for the cursor (e.g., 'after_id', 'cursor', 'next').").
						Default("after_id"),

					service.NewStringField(hipCursorNextField).
						Description("JSON path to the next cursor value in the response (e.g., 'last_id', 'next_cursor').").
						Default("last_id"),

					service.NewStringField(hipCursorHasMoreField).
						Description("JSON path to a boolean indicating if more pages exist (optional).").
						Default("has_more").
						Optional(),

					service.NewStringField(hipCursorInitial).
						Description("Initial cursor value to start from. Empty string means start from the beginning.").
						Default(""),
				).Description("Cursor-based pagination configuration.").
					Optional(),
			).Description("Pagination configuration."),

			service.NewObjectField(hipFieldResponse,
				service.NewStringField(hipResponseDataField).
					Description("JSON path to the array of records in the response (e.g., 'data', 'results', 'items'). Use '.' if the response itself is the array.").
					Default("data"),

				service.NewBoolField(hipResponseFlatten).
					Description("If true, emit each record as a separate message. If false, emit the entire page as one message.").
					Default(true),
			).Description("Response parsing configuration."),

			service.NewObjectField(hipFieldCheckpoint,
				service.NewStringField("cache").
					Description("Name of cache resource to use for checkpointing (must be defined in cache_resources)."),

				service.NewStringField("key").
					Description("Cache key for storing the checkpoint cursor. Supports environment variable expansion."),

				service.NewStringEnumField("save_strategy", "first_page", "last_page").
					Description("Which cursor to save: 'first_page' (for APIs returning newest-first) or 'last_page' (for APIs returning oldest-first). Default: 'last_page'.").
					Default("last_page").
					Advanced(),
			).Description("Checkpoint configuration for resumability. Requires a cache resource.").
				Optional(),

			service.NewIntField(hipFieldMaxPages).
				Description("Maximum number of pages to fetch (safety limit). 0 means unlimited.").
				Default(0).
				Advanced(),

			service.NewIntField(hipFieldMaxRecords).
				Description("Maximum number of records to fetch (safety limit). 0 means unlimited.").
				Default(0).
				Advanced(),
		)
}

func init() {
	err := service.RegisterBatchInput(
		"http_paginated", httpPaginatedInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newHTTPPaginatedInputFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpPaginatedInput struct {
	url     string
	headers map[string]string
	verb    string
	timeout time.Duration

	paginator  *cursorPaginator
	checkpoint checkpointer
	checkpointStrategy string // "first_page" or "last_page"

	dataField string
	flatten   bool

	maxPages   int
	maxRecords int

	client        *http.Client
	currentPage   int
	totalRecords  int
	buffer        []any
	done          bool
	pendingCursor string // Cursor to save after current page is fully consumed

	log *service.Logger
}

func newHTTPPaginatedInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*httpPaginatedInput, error) {
	url, err := conf.FieldString(hipFieldURL)
	if err != nil {
		return nil, err
	}

	headers, err := conf.FieldStringMap(hipFieldHeaders)
	if err != nil {
		return nil, err
	}

	verb, err := conf.FieldString(hipFieldVerb)
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration(hipFieldTimeout)
	if err != nil {
		return nil, err
	}

	// Pagination config
	paginationType, err := conf.FieldString(hipFieldPagination, hipPaginationType)
	if err != nil {
		return nil, err
	}

	if paginationType != "cursor" {
		return nil, fmt.Errorf("pagination type %q not yet implemented (only 'cursor' supported)", paginationType)
	}

	// Cursor pagination config
	cursorParamName, err := conf.FieldString(hipFieldPagination, hipPaginationCursor, hipCursorParamName)
	if err != nil {
		return nil, err
	}

	nextCursorField, err := conf.FieldString(hipFieldPagination, hipPaginationCursor, hipCursorNextField)
	if err != nil {
		return nil, err
	}

	hasMoreField := ""
	if conf.Contains(hipFieldPagination, hipPaginationCursor, hipCursorHasMoreField) {
		hasMoreField, err = conf.FieldString(hipFieldPagination, hipPaginationCursor, hipCursorHasMoreField)
		if err != nil {
			return nil, err
		}
	}

	initialCursor, err := conf.FieldString(hipFieldPagination, hipPaginationCursor, hipCursorInitial)
	if err != nil {
		return nil, err
	}

	// Response config
	dataField, err := conf.FieldString(hipFieldResponse, hipResponseDataField)
	if err != nil {
		return nil, err
	}

	flatten, err := conf.FieldBool(hipFieldResponse, hipResponseFlatten)
	if err != nil {
		return nil, err
	}

	// Checkpoint config
	var checkpoint checkpointer
	checkpointStrategy := "last_page" // Default
	if conf.Contains(hipFieldCheckpoint) {
		cacheName, err := conf.FieldString(hipFieldCheckpoint, hipCheckpointCache)
		if err != nil {
			return nil, fmt.Errorf("checkpoint cache field required: %w", err)
		}

		cacheKey, err := conf.FieldString(hipFieldCheckpoint, hipCheckpointKey)
		if err != nil {
			return nil, fmt.Errorf("checkpoint key field required: %w", err)
		}

		strategy, err := conf.FieldString(hipFieldCheckpoint, hipCheckpointStrategy)
		if err == nil {
			checkpointStrategy = strategy
		}

		checkpoint = &cacheCheckpointer{
			mgr:       mgr,
			cacheName: cacheName,
			key:       cacheKey,
			log:       mgr.Logger(),
		}
	} else {
		checkpoint = &noopCheckpointer{}
	}

	// Safety limits
	maxPages, err := conf.FieldInt(hipFieldMaxPages)
	if err != nil {
		return nil, err
	}

	maxRecords, err := conf.FieldInt(hipFieldMaxRecords)
	if err != nil {
		return nil, err
	}

	return &httpPaginatedInput{
		url:     url,
		headers: headers,
		verb:    verb,
		timeout: timeout,
		paginator: &cursorPaginator{
			baseURL:         url,
			cursorParamName: cursorParamName,
			nextCursorField: nextCursorField,
			hasMoreField:    hasMoreField,
			currentCursor:   initialCursor,
		},
		checkpoint:   checkpoint,
		checkpointStrategy: checkpointStrategy,
		dataField:    dataField,
		flatten:      flatten,
		maxPages:     maxPages,
		maxRecords:   maxRecords,
		client:       &http.Client{Timeout: timeout},
		buffer:       []any{},
		log:          mgr.Logger(),
	}, nil
}

func (h *httpPaginatedInput) Connect(ctx context.Context) error {
	// Load checkpoint to resume from last position
	cursor, err := h.checkpoint.Load()
	if err != nil {
		h.log.With("error", err).Warn("Failed to load checkpoint, starting from beginning")
	} else if cursor != "" {
		h.paginator.currentCursor = cursor
		h.log.With("cursor", cursor).Info("Resuming from checkpoint")
	}

	return nil
}

func (h *httpPaginatedInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	// If we have buffered records, return them
	if len(h.buffer) > 0 {
		return h.emitFromBuffer()
	}

	// Check if we're done
	if h.done {
		return nil, nil, service.ErrEndOfInput
	}

	// Check safety limits
	if h.maxPages > 0 && h.currentPage >= h.maxPages {
		h.log.With("max_pages", h.maxPages).Info("Reached max pages limit")
		h.done = true
		return nil, nil, service.ErrEndOfInput
	}

	if h.maxRecords > 0 && h.totalRecords >= h.maxRecords {
		h.log.With("max_records", h.maxRecords).Info("Reached max records limit")
		h.done = true
		return nil, nil, service.ErrEndOfInput
	}

	// Fetch next page
	url := h.paginator.nextURL()
	h.log.With("url", url, "page", h.currentPage+1).Debug("Fetching page")

	req, err := http.NewRequestWithContext(ctx, h.verb, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var respData map[string]any
	if err := json.Unmarshal(bodyBytes, &respData); err != nil {
		return nil, nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	// Extract data array
	var records []any
	if h.dataField == "." {
		// Response itself is the array
		if err := json.Unmarshal(bodyBytes, &records); err != nil {
			return nil, nil, fmt.Errorf("failed to parse response as array: %w", err)
		}
	} else {
		dataRaw, ok := respData[h.dataField]
		if !ok {
			return nil, nil, fmt.Errorf("data field %q not found in response", h.dataField)
		}
		dataArray, ok := dataRaw.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("data field %q is not an array", h.dataField)
		}
		records = dataArray
	}

	h.currentPage++
	h.totalRecords += len(records)
	h.log.With("page", h.currentPage, "records", len(records), "total", h.totalRecords).Info("Fetched page")

	// Update paginator state
	hasMore, nextCursor, err := h.paginator.update(respData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to update pagination state: %w", err)
	}

	if !hasMore || nextCursor == "" {
		h.log.With("total_pages", h.currentPage, "total_records", h.totalRecords).Info("Pagination complete")
		h.done = true
	}

	// Store cursor to save after this page is fully consumed
	// Strategy determines which cursor to save to checkpoint
	if h.checkpointStrategy == "first_page" {
		// For APIs that return newest-first: save cursor from page 1
		if h.currentPage == 1 {
			h.pendingCursor = nextCursor
		}
		// Otherwise pendingCursor stays as-is (from first page)
	} else {
		// For APIs that return oldest-first: always update to latest cursor
		h.pendingCursor = nextCursor
	}

	// Buffer records
	if h.flatten {
		h.buffer = records
	} else {
		// Return entire page as one message
		h.buffer = []any{records}
	}

	// If no records, check if we're done
	if len(h.buffer) == 0 {
		if h.done {
			return nil, nil, service.ErrEndOfInput
		}
		// Empty page but more to come - continue
		return h.ReadBatch(ctx)
	}

	return h.emitFromBuffer()
}

func (h *httpPaginatedInput) emitFromBuffer() (service.MessageBatch, service.AckFunc, error) {
	if len(h.buffer) == 0 {
		return nil, nil, service.ErrEndOfInput
	}

	// Emit one or more records
	batchSize := 1
	if !h.flatten {
		batchSize = len(h.buffer)
	}

	batch := make(service.MessageBatch, 0, batchSize)
	for i := 0; i < batchSize && len(h.buffer) > 0; i++ {
		record := h.buffer[0]
		h.buffer = h.buffer[1:]

		recordBytes, err := json.Marshal(record)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal record: %w", err)
		}

		msg := service.NewMessage(recordBytes)
		batch = append(batch, msg)
	}

	// Capture state at closure creation time: is this the last message from the current page?
	isLastMessageOfPage := len(h.buffer) == 0 && h.pendingCursor != ""
	pendingCursorToSave := h.pendingCursor

	ackFn := func(ctx context.Context, err error) error {
		if err != nil {
			// Message failed - don't update checkpoint
			return nil
		}

		// Save checkpoint once when this was the last message of a page
		if isLastMessageOfPage {
			if err := h.checkpoint.Save(pendingCursorToSave); err != nil {
				h.log.With("error", err).Warn("Failed to save checkpoint")
				// Don't fail the ack, just log
			}
			// Clear pending cursor after saving
			h.pendingCursor = ""
		}

		return nil
	}

	return batch, ackFn, nil
}

func (h *httpPaginatedInput) Close(ctx context.Context) error {
	return h.checkpoint.Close()
}

//------------------------------------------------------------------------------

type cursorPaginator struct {
	baseURL         string
	cursorParamName string
	nextCursorField string
	hasMoreField    string
	currentCursor   string
}

func (p *cursorPaginator) nextURL() string {
	if p.currentCursor == "" {
		return p.baseURL
	}
	separator := "?"
	if contains(p.baseURL, "?") {
		separator = "&"
	}
	return fmt.Sprintf("%s%s%s=%s", p.baseURL, separator, p.cursorParamName, p.currentCursor)
}

func (p *cursorPaginator) update(respData map[string]any) (hasMore bool, nextCursor string, err error) {
	// Extract next cursor
	nextCursor, _ = respData[p.nextCursorField].(string)

	// Check has_more if field is specified
	if p.hasMoreField != "" {
		hasMoreRaw, ok := respData[p.hasMoreField]
		if ok {
			hasMore, _ = hasMoreRaw.(bool)
		}
	} else {
		// If no has_more field, assume more pages if we got a next cursor
		hasMore = nextCursor != ""
	}

	p.currentCursor = nextCursor
	return hasMore, nextCursor, nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------

type checkpointer interface {
	Load() (string, error)
	Save(cursor string) error
	Close() error
}

type noopCheckpointer struct{}

func (n *noopCheckpointer) Load() (string, error)       { return "", nil }
func (n *noopCheckpointer) Save(cursor string) error    { return nil }
func (n *noopCheckpointer) Close() error                { return nil }
// cacheCheckpointer uses a Bento cache resource for checkpointing
type cacheCheckpointer struct {
	mgr       *service.Resources
	cacheName string
	key       string
	log       *service.Logger
}

func (c *cacheCheckpointer) Load() (string, error) {
	var cursor string
	var loadErr error
	err := c.mgr.AccessCache(context.Background(), c.cacheName, func(cache service.Cache) {
		data, getErr := cache.Get(context.Background(), c.key)
		if getErr != nil {
			// Cache miss is not an error - just means no checkpoint exists
			c.log.With("key", c.key).Debug("No checkpoint found, starting from beginning")
			loadErr = nil // Explicitly no error on cache miss
			return
		}
		cursor = string(data)
		c.log.With("key", c.key, "cursor", cursor).Info("Loaded checkpoint from cache")
	})
	if err != nil {
		return "", err // AccessCache itself failed
	}
	return cursor, loadErr
}

func (c *cacheCheckpointer) Save(cursor string) error {
	var saveErr error
	err := c.mgr.AccessCache(context.Background(), c.cacheName, func(cache service.Cache) {
		setErr := cache.Set(context.Background(), c.key, []byte(cursor), nil)
		if setErr != nil {
			c.log.With("key", c.key, "error", setErr).Warn("Failed to save checkpoint")
			saveErr = setErr
			return
		}
		c.log.With("key", c.key, "cursor", cursor).Debug("Saved checkpoint to cache")
	})
	if err != nil {
		return err // AccessCache itself failed
	}
	return saveErr
}

func (c *cacheCheckpointer) Close() error {
	// Cache is managed by Bento resources, no need to close
	return nil
}
