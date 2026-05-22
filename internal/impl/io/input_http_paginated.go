package io

import (
	"context"
	"encoding/json"
	"fmt"
	goio "io"
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

	hipPaginationTimeWindow   = "time_window"
	hipTimeWindowStartParam   = "start_param"
	hipTimeWindowEndParam     = "end_param"
	hipTimeWindowFormat       = "format"
	hipTimeWindowSize         = "window_size"
	hipTimeWindowLag          = "lag"
	hipTimeWindowInitial      = "initial_window"
	hipTimeWindowCursor       = "cursor"

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
				service.NewStringEnumField(hipPaginationType, "cursor", "time_window").
					Description("Pagination strategy type: 'cursor' for cursor-based APIs, 'time_window' for time-range based APIs with optional cursor pagination within windows.").
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
				).Description("Cursor-based pagination configuration. Required when type is 'cursor'.").
					Optional(),

				service.NewObjectField(hipPaginationTimeWindow,
					service.NewStringField(hipTimeWindowStartParam).
						Description("Query parameter name for window start time (e.g., 'created_at.gte', 'since', 'start_time').").
						Default("created_at.gte"),

					service.NewStringField(hipTimeWindowEndParam).
						Description("Query parameter name for window end time (e.g., 'created_at.lt', 'until', 'end_time').").
						Default("created_at.lt"),

					service.NewStringField(hipTimeWindowFormat).
						Description("Go time format string for formatting timestamps in URL (e.g., '2006-01-02T15:04:05Z07:00' for RFC3339).").
						Default("2006-01-02T15:04:05Z07:00"),

					service.NewDurationField(hipTimeWindowSize).
						Description("Size of each time window to process (e.g., '2m', '5m', '1h').").
						Default("2m"),

					service.NewDurationField(hipTimeWindowLag).
						Description("Lag buffer - how far behind current time to process (e.g., '2m' means process up to 2 minutes ago). This allows time for events to be indexed by the API.").
						Default("2m"),

					service.NewDurationField(hipTimeWindowInitial).
						Description("If no checkpoint exists, how far back to start from current time (e.g., '-1h' for 1 hour ago). Negative durations go backwards in time.").
						Default("-1h"),

					service.NewObjectField(hipTimeWindowCursor,
						service.NewStringField(hipCursorParamName).
							Description("Query parameter name for the cursor (e.g., 'after_id').").
							Default("after_id"),

						service.NewStringField(hipCursorNextField).
							Description("JSON path to the next cursor value in the response (e.g., 'last_id').").
							Default("last_id"),

						service.NewStringField(hipCursorHasMoreField).
							Description("JSON path to a boolean indicating if more pages exist (optional).").
							Default("has_more").
							Optional(),
					).Description("Optional cursor pagination within each time window. Useful when a single window may contain more records than the page size limit.").
						Optional(),
				).Description("Time window pagination configuration. Required when type is 'time_window'. Enables parallel processing by reserving time windows in the checkpoint.").
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

type paginator interface {
	nextURL() string
	update(respData map[string]any) (hasMore bool, nextCursor string, err error)
}

type httpPaginatedInput struct {
	url     string
	headers map[string]string
	verb    string
	timeout time.Duration

	paginator          paginator
	timeWindowPag      *timeWindowPaginator // Set only for time_window type
	checkpoint         checkpointer
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

	var pag paginator
	var timeWindowPag *timeWindowPaginator

	switch paginationType {
	case "cursor":
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

		pag = &cursorPaginator{
			baseURL:         url,
			cursorParamName: cursorParamName,
			nextCursorField: nextCursorField,
			hasMoreField:    hasMoreField,
			currentCursor:   initialCursor,
		}

	case "time_window":
		// Time window pagination config
		startParam, err := conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowStartParam)
		if err != nil {
			return nil, err
		}

		endParam, err := conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowEndParam)
		if err != nil {
			return nil, err
		}

		format, err := conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowFormat)
		if err != nil {
			return nil, err
		}

		windowSize, err := conf.FieldDuration(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowSize)
		if err != nil {
			return nil, err
		}

		lag, err := conf.FieldDuration(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowLag)
		if err != nil {
			return nil, err
		}

		initialWindow, err := conf.FieldDuration(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowInitial)
		if err != nil {
			return nil, err
		}

		twPag := &timeWindowPaginator{
			baseURL:       url,
			startParam:    startParam,
			endParam:      endParam,
			format:        format,
			windowSize:    windowSize,
			lag:           lag,
			initialWindow: initialWindow,
		}

		// Check if cursor pagination within windows is configured
		if conf.Contains(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowCursor) {
			cursorParamName, err := conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowCursor, hipCursorParamName)
			if err != nil {
				return nil, err
			}

			nextCursorField, err := conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowCursor, hipCursorNextField)
			if err != nil {
				return nil, err
			}

			hasMoreField := ""
			if conf.Contains(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowCursor, hipCursorHasMoreField) {
				hasMoreField, err = conf.FieldString(hipFieldPagination, hipPaginationTimeWindow, hipTimeWindowCursor, hipCursorHasMoreField)
				if err != nil {
					return nil, err
				}
			}

			twPag.hasCursor = true
			twPag.cursorParamName = cursorParamName
			twPag.nextCursorField = nextCursorField
			twPag.hasMoreField = hasMoreField
		}

		pag = twPag
		timeWindowPag = twPag

	default:
		return nil, fmt.Errorf("pagination type %q not supported", paginationType)
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
		url:                url,
		headers:            headers,
		verb:               verb,
		timeout:            timeout,
		paginator:          pag,
		timeWindowPag:      timeWindowPag,
		checkpoint:         checkpoint,
		checkpointStrategy: checkpointStrategy,
		dataField:          dataField,
		flatten:            flatten,
		maxPages:           maxPages,
		maxRecords:         maxRecords,
		client:             &http.Client{Timeout: timeout},
		buffer:             []any{},
		log:                mgr.Logger(),
	}, nil
}

func (h *httpPaginatedInput) Connect(ctx context.Context) error {
	// Load checkpoint to resume from last position
	checkpointValue, err := h.checkpoint.Load()
	if err != nil {
		h.log.With("error", err).Warn("Failed to load checkpoint, starting from beginning")
	}

	if h.timeWindowPag != nil {
		// Time window pagination - initialize window from checkpoint
		if err := h.timeWindowPag.initializeWindow(checkpointValue); err != nil {
			return fmt.Errorf("failed to initialize time window: %w", err)
		}

		// Check if already caught up (no window to process)
		if h.timeWindowPag.isDone() {
			h.log.Info("Already caught up to current time, no data to fetch")
			h.done = true
		} else {
			h.log.With(
				"window_start", h.timeWindowPag.windowStart.Format(time.RFC3339),
				"window_end", h.timeWindowPag.windowEnd.Format(time.RFC3339),
			).Info("Processing time window")
		}

		// Save new checkpoint BEFORE fetching (window reservation)
		newCheckpoint := h.timeWindowPag.getCheckpoint()
		if err := h.checkpoint.Save(newCheckpoint); err != nil {
			h.log.With("error", err).Warn("Failed to save initial checkpoint")
		} else {
			h.log.With("checkpoint", newCheckpoint).Info("Reserved time window")
		}
	} else if cursorPag, ok := h.paginator.(*cursorPaginator); ok {
		// Cursor pagination - set initial cursor
		if checkpointValue != "" {
			cursorPag.currentCursor = checkpointValue
			h.log.With("cursor", checkpointValue).Info("Resuming from checkpoint")
		}
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
		bodyBytes, _ := goio.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := goio.ReadAll(resp.Body)
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

	// Store cursor to save after this page is fully consumed (only for cursor pagination)
	// For time windows, checkpoint is already saved at the beginning (window reservation)
	if h.timeWindowPag == nil {
		// Pure cursor pagination - save cursor based on strategy
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

type timeWindowPaginator struct {
	baseURL       string
	startParam    string
	endParam      string
	format        string
	windowSize    time.Duration
	lag           time.Duration
	initialWindow time.Duration

	// Window state
	windowStart time.Time
	windowEnd   time.Time

	// Optional cursor pagination within windows
	hasCursor       bool
	cursorParamName string
	nextCursorField string
	hasMoreField    string
	currentCursor   string
}

func (p *timeWindowPaginator) initializeWindow(checkpointTime string) error {
	if checkpointTime != "" {
		// Parse checkpoint as timestamp
		t, err := time.Parse(p.format, checkpointTime)
		if err != nil {
			return fmt.Errorf("failed to parse checkpoint timestamp %q: %w", checkpointTime, err)
		}
		p.windowStart = t
	} else {
		// No checkpoint - use initial window (e.g., -1h from current time)
		p.windowStart = time.Now().UTC().Add(p.initialWindow)
	}

	// Calculate window end: current time minus lag
	p.windowEnd = time.Now().UTC().Add(-p.lag)

	// Clamp window end to windowStart + windowSize to avoid too large windows
	maxWindowEnd := p.windowStart.Add(p.windowSize)
	if p.windowEnd.After(maxWindowEnd) {
		p.windowEnd = maxWindowEnd
	}

	return nil
}

func (p *timeWindowPaginator) nextURL() string {
	// Build URL with time window parameters
	separator := "?"
	if contains(p.baseURL, "?") {
		separator = "&"
	}

	url := fmt.Sprintf("%s%s%s=%s&%s=%s",
		p.baseURL,
		separator,
		p.startParam, p.windowStart.Format(p.format),
		p.endParam, p.windowEnd.Format(p.format))

	// Add cursor if we have one
	if p.hasCursor && p.currentCursor != "" {
		url = fmt.Sprintf("%s&%s=%s", url, p.cursorParamName, p.currentCursor)
	}

	return url
}

func (p *timeWindowPaginator) update(respData map[string]any) (hasMore bool, nextCursor string, err error) {
	if !p.hasCursor {
		// No cursor pagination - single page per window
		return false, "", nil
	}

	// Extract next cursor for pagination within window
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

func (p *timeWindowPaginator) getCheckpoint() string {
	// Return window end as the checkpoint (next window will start from here)
	return p.windowEnd.Format(p.format)
}

func (p *timeWindowPaginator) isDone() bool {
	// Done if window start >= window end (caught up to current time minus lag)
	return !p.windowStart.Before(p.windowEnd)
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
