package io

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestHTTPPaginatedInputConfigParsing(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		expectError bool
	}{
		{
			name: "basic cursor pagination",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
    has_more_field: has_more
response:
  data_field: data
  flatten: true
`,
			expectError: false,
		},
		{
			name: "with checkpoint",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  cache: my_cache
  key: my_checkpoint
`,
			expectError: false,
		},
		{
			name: "with checkpoint first_page strategy",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  cache: my_cache
  key: my_checkpoint
  save_strategy: first_page
`,
			expectError: false,
		},
		{
			name: "with checkpoint last_page strategy",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  cache: my_cache
  key: my_checkpoint
  save_strategy: last_page
`,
			expectError: false,
		},
		{
			name: "with max limits",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
max_pages: 10
max_records: 1000
`,
			expectError: false,
		},
		{
			name: "with custom headers",
			config: `
url: https://api.example.com/data
headers:
  Authorization: "Bearer token123"
  X-Custom-Header: "value"
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`,
			expectError: false,
		},
		{
			name: "with POST method",
			config: `
url: https://api.example.com/data
verb: POST
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`,
			expectError: false,
		},
		{
			name: "with initial cursor",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
    initial_cursor: "starting_point"
response:
  data_field: data
`,
			expectError: false,
		},
		{
			name: "flatten false",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
  flatten: false
`,
			expectError: false,
		},
		{
			name: "missing url",
			config: `
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`,
			expectError: true,
		},
		{
			name: "checkpoint missing cache",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  key: my_checkpoint
`,
			expectError: true,
		},
		{
			name: "checkpoint missing key",
			config: `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  cache: my_cache
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := httpPaginatedInputSpec()
			env := service.NewEnvironment()

			parsed, err := spec.ParseYAML(tt.config, env)

			if tt.expectError {
				assert.Error(t, err, "Expected config parsing to fail")
			} else {
				require.NoError(t, err, "Expected config parsing to succeed")
				assert.NotNil(t, parsed)
			}
		})
	}
}

func TestHTTPPaginatedInputDefaults(t *testing.T) {
	config := `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`

	spec := httpPaginatedInputSpec()
	env := service.NewEnvironment()

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	// Verify defaults are applied
	verb, err := parsed.FieldString("verb")
	require.NoError(t, err)
	assert.Equal(t, "GET", verb)

	timeout, err := parsed.FieldDuration("timeout")
	require.NoError(t, err)
	assert.Equal(t, "30s", timeout.String())

	flatten, err := parsed.FieldBool("response", "flatten")
	require.NoError(t, err)
	assert.True(t, flatten)

	dataField, err := parsed.FieldString("response", "data_field")
	require.NoError(t, err)
	assert.Equal(t, "data", dataField)

	maxPages, err := parsed.FieldInt("max_pages")
	require.NoError(t, err)
	assert.Equal(t, 0, maxPages)

	maxRecords, err := parsed.FieldInt("max_records")
	require.NoError(t, err)
	assert.Equal(t, 0, maxRecords)
}

func TestHTTPPaginatedInputCheckpointStrategyDefault(t *testing.T) {
	config := `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
checkpoint:
  cache: my_cache
  key: my_key
`

	spec := httpPaginatedInputSpec()
	env := service.NewEnvironment()

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	// Verify default save_strategy is last_page
	strategy, err := parsed.FieldString("checkpoint", "save_strategy")
	require.NoError(t, err)
	assert.Equal(t, "last_page", strategy)
}

func TestHTTPPaginatedInputCursorDefaults(t *testing.T) {
	config := `
url: https://api.example.com/data
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`

	spec := httpPaginatedInputSpec()
	env := service.NewEnvironment()

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	// Verify cursor defaults
	initialCursor, err := parsed.FieldString("pagination", "cursor", "initial_cursor")
	require.NoError(t, err)
	assert.Equal(t, "", initialCursor)
}

func TestHTTPPaginatedInputURLValidation(t *testing.T) {
	config := `
url: not-a-valid-url
pagination:
  type: cursor
  cursor:
    param_name: after_id
    next_cursor_field: last_id
response:
  data_field: data
`

	spec := httpPaginatedInputSpec()
	env := service.NewEnvironment()

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err) // URL field doesn't validate format during parse

	// The actual validation would happen when creating the input
	// This is testing that the config can be parsed, even if invalid
	assert.NotNil(t, parsed)
}

func TestExtractPath(t *testing.T) {
	tests := []struct {
		name     string
		resp     map[string]any
		path     string
		wantVal  any
		wantOK   bool
	}{
		{
			name:    "flat top-level field (e.g. anthropic last_id)",
			resp:    map[string]any{"last_id": "abc123", "has_more": true},
			path:    "last_id",
			wantVal: "abc123",
			wantOK:  true,
		},
		{
			name:    "nested field (e.g. airtable pagination.next)",
			resp:    map[string]any{"pagination": map[string]any{"next": "cursor_xyz"}},
			path:    "pagination.next",
			wantVal: "cursor_xyz",
			wantOK:  true,
		},
		{
			name:    "nested field absent - last page",
			resp:    map[string]any{"pagination": map[string]any{}},
			path:    "pagination.next",
			wantVal: nil,
			wantOK:  false,
		},
		{
			name:    "intermediate segment missing entirely",
			resp:    map[string]any{"events": []any{}},
			path:    "pagination.next",
			wantVal: nil,
			wantOK:  false,
		},
		{
			name:    "intermediate segment not an object",
			resp:    map[string]any{"pagination": "not-an-object"},
			path:    "pagination.next",
			wantVal: nil,
			wantOK:  false,
		},
		{
			name:    "deeply nested path",
			resp:    map[string]any{"a": map[string]any{"b": map[string]any{"c": "deep"}}},
			path:    "a.b.c",
			wantVal: "deep",
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotOK := extractPathOK(tt.resp, tt.path)
			assert.Equal(t, tt.wantOK, gotOK)
			assert.Equal(t, tt.wantVal, gotVal)
			assert.Equal(t, tt.wantVal, extractPath(tt.resp, tt.path))
		})
	}
}

func TestCursorPaginatorUpdate_FlatFields(t *testing.T) {
	// Anthropic-shaped response: flat last_id/has_more, must keep working unchanged.
	p := &cursorPaginator{nextCursorField: "last_id", hasMoreField: "has_more"}

	hasMore, cursor, err := p.update(map[string]any{"last_id": "page1cursor", "has_more": true})
	require.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, "page1cursor", cursor)

	hasMore, cursor, err = p.update(map[string]any{"last_id": "", "has_more": false})
	require.NoError(t, err)
	assert.False(t, hasMore)
	assert.Equal(t, "", cursor)
}

func TestCursorPaginatorUpdate_NestedFieldNoHasMore(t *testing.T) {
	// Airtable-shaped response: nested pagination.next, no has_more boolean at all.
	// This is the exact bug that silently dropped ~74% of events: before the fix,
	// respData["pagination.next"] was always a flat-key miss, so nextCursor was
	// always "", and pagination stopped after page 1 regardless of real data.
	p := &cursorPaginator{nextCursorField: "pagination.next"}

	hasMore, cursor, err := p.update(map[string]any{
		"events":     []any{"e1", "e2"},
		"pagination": map[string]any{"next": "cursor_page2"},
	})
	require.NoError(t, err)
	assert.True(t, hasMore, "must detect more pages from a nested cursor")
	assert.Equal(t, "cursor_page2", cursor)

	// Last page: cursor genuinely absent -> must terminate.
	hasMore, cursor, err = p.update(map[string]any{
		"events":     []any{"e3"},
		"pagination": map[string]any{},
	})
	require.NoError(t, err)
	assert.False(t, hasMore, "must terminate when the nested cursor is absent")
	assert.Equal(t, "", cursor)
}