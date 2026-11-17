// +build integration

package io_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
)

// TestHTTPPaginatedInputIntegrationFullWorkflow tests the complete end-to-end workflow
// with a mock HTTP server simulating a paginated API
func TestHTTPPaginatedInputIntegrationFullWorkflow(t *testing.T) {
	// Create a mock paginated API server
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		// Verify headers
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))

		afterID := r.URL.Query().Get("after_id")

		var resp map[string]any
		switch afterID {
		case "":
			// Page 1: IDs 1-5
			resp = map[string]any{
				"data": []any{
					map[string]any{"id": "1", "value": "a"},
					map[string]any{"id": "2", "value": "b"},
					map[string]any{"id": "3", "value": "c"},
					map[string]any{"id": "4", "value": "d"},
					map[string]any{"id": "5", "value": "e"},
				},
				"last_id":  "5",
				"has_more": true,
			}
		case "5":
			// Page 2: IDs 6-10
			resp = map[string]any{
				"data": []any{
					map[string]any{"id": "6", "value": "f"},
					map[string]any{"id": "7", "value": "g"},
					map[string]any{"id": "8", "value": "h"},
					map[string]any{"id": "9", "value": "i"},
					map[string]any{"id": "10", "value": "j"},
				},
				"last_id":  "10",
				"has_more": true,
			}
		case "10":
			// Page 3: IDs 11-12 (final page)
			resp = map[string]any{
				"data": []any{
					map[string]any{"id": "11", "value": "k"},
					map[string]any{"id": "12", "value": "l"},
				},
				"last_id":  "12",
				"has_more": false,
			}
		default:
			t.Fatalf("Unexpected after_id: %s", afterID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	mgr := mock.NewManager()
	mgr.Caches["integration_cache"] = map[string]mock.CacheItem{}

	conf, err := testutil.InputFromYAML(fmt.Sprintf(`
http_paginated:
  url: %s
  headers:
    Authorization: "Bearer test-token"
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
    cache: integration_cache
    key: integration_test
  timeout: 5s
`, server.URL))
	require.NoError(t, err)

	input, err := mgr.NewInput(conf)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = input.Connect(ctx)
	require.NoError(t, err)

	// Read all messages and verify
	var receivedIDs []string
	for {
		batch, ack, err := input.ReadBatch(ctx)
		if err != nil {
			break
		}

		for _, msg := range batch {
			bytes, _ := msg.AsBytes()
			var record map[string]any
			require.NoError(t, json.Unmarshal(bytes, &record))
			receivedIDs = append(receivedIDs, record["id"].(string))
		}

		require.NoError(t, ack(ctx, nil))
	}

	require.NoError(t, input.Close(ctx))

	// Verify we got all 12 records in order
	assert.Len(t, receivedIDs, 12)
	for i := 1; i <= 12; i++ {
		assert.Equal(t, fmt.Sprintf("%d", i), receivedIDs[i-1])
	}

	// Verify we made exactly 3 API calls
	assert.Equal(t, 3, callCount)

	// Verify checkpoint was saved to last cursor
	checkpointItem, exists := mgr.Caches["integration_cache"]["integration_test"]
	require.True(t, exists)
	assert.Equal(t, "12", checkpointItem.Value)
}

// TestHTTPPaginatedInputIntegrationResumeAfterFailure tests that the input
// can resume from a checkpoint after a simulated failure
func TestHTTPPaginatedInputIntegrationResumeAfterFailure(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		afterID := r.URL.Query().Get("after_id")

		var resp map[string]any
		switch afterID {
		case "":
			resp = map[string]any{
				"data":     []any{map[string]any{"id": "1"}},
				"last_id":  "1",
				"has_more": true,
			}
		case "1":
			resp = map[string]any{
				"data":     []any{map[string]any{"id": "2"}},
				"last_id":  "2",
				"has_more": true,
			}
		case "2":
			resp = map[string]any{
				"data":     []any{map[string]any{"id": "3"}},
				"last_id":  "3",
				"has_more": false,
			}
		default:
			t.Fatalf("Unexpected after_id: %s", afterID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	mgr := mock.NewManager()
	mgr.Caches["resume_cache"] = map[string]mock.CacheItem{}

	conf, err := testutil.InputFromYAML(fmt.Sprintf(`
http_paginated:
  url: %s
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
    cache: resume_cache
    key: resume_test
`, server.URL))
	require.NoError(t, err)

	ctx := context.Background()

	// First run: read page 1 and simulate failure
	input1, err := mgr.NewInput(conf)
	require.NoError(t, err)
	require.NoError(t, input1.Connect(ctx))

	batch1, ack1, err := input1.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch1, 1)

	// Ack to save checkpoint
	require.NoError(t, ack1(ctx, nil))

	// Simulate failure by closing input
	require.NoError(t, input1.Close(ctx))

	// Verify checkpoint was saved
	checkpointItem, exists := mgr.Caches["resume_cache"]["resume_test"]
	require.True(t, exists)
	assert.Equal(t, "1", checkpointItem.Value)

	// Reset call count to track resumed calls
	callCount = 0

	// Second run: resume from checkpoint
	input2, err := mgr.NewInput(conf)
	require.NoError(t, err)
	require.NoError(t, input2.Connect(ctx))

	// Read remaining pages
	var receivedIDs []string
	for {
		batch, ack, err := input2.ReadBatch(ctx)
		if err != nil {
			break
		}

		for _, msg := range batch {
			bytes, _ := msg.AsBytes()
			var record map[string]any
			require.NoError(t, json.Unmarshal(bytes, &record))
			receivedIDs = append(receivedIDs, record["id"].(string))
		}

		require.NoError(t, ack(ctx, nil))
	}

	require.NoError(t, input2.Close(ctx))

	// Should have only fetched pages 2 and 3
	assert.Equal(t, []string{"2", "3"}, receivedIDs)
	assert.Equal(t, 2, callCount, "Should only make 2 API calls after resuming")

	// Final checkpoint should be at end
	finalCheckpoint, _ := mgr.Caches["resume_cache"]["resume_test"]
	assert.Equal(t, "3", finalCheckpoint.Value)
}

// TestHTTPPaginatedInputIntegrationNewestFirstAPI tests the first_page
// checkpoint strategy with a newest-first API
func TestHTTPPaginatedInputIntegrationNewestFirstAPI(t *testing.T) {
	// Simulate an API that returns newest records first (like activity feeds)
	apiState := []map[string]any{
		{"id": "100", "timestamp": "2024-01-01T10:00:00Z"},
		{"id": "99", "timestamp": "2024-01-01T09:59:00Z"},
		{"id": "98", "timestamp": "2024-01-01T09:58:00Z"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		afterID := r.URL.Query().Get("after_id")

		var resp map[string]any
		if afterID == "" {
			// Return all current records
			resp = map[string]any{
				"data":     apiState,
				"last_id":  apiState[len(apiState)-1]["id"],
				"has_more": false,
			}
		} else {
			// Return records after the cursor
			var newRecords []map[string]any
			for _, record := range apiState {
				if record["id"].(string) > afterID {
					newRecords = append(newRecords, record)
				}
			}

			resp = map[string]any{
				"data":     newRecords,
				"last_id":  "",
				"has_more": false,
			}
			if len(newRecords) > 0 {
				resp["last_id"] = newRecords[len(newRecords)-1]["id"]
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	mgr := mock.NewManager()
	mgr.Caches["newest_first_cache"] = map[string]mock.CacheItem{}

	conf, err := testutil.InputFromYAML(fmt.Sprintf(`
http_paginated:
  url: %s
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
    cache: newest_first_cache
    key: newest_first_test
    save_strategy: first_page
`, server.URL))
	require.NoError(t, err)

	ctx := context.Background()

	// First run: fetch all current records
	input1, err := mgr.NewInput(conf)
	require.NoError(t, err)
	require.NoError(t, input1.Connect(ctx))

	count1 := 0
	for {
		batch, ack, err := input1.ReadBatch(ctx)
		if err != nil {
			break
		}
		count1 += len(batch)
		require.NoError(t, ack(ctx, nil))
	}
	require.NoError(t, input1.Close(ctx))

	assert.Equal(t, 3, count1)

	// Checkpoint should be the last_id from the FIRST page (98)
	checkpoint1, _ := mgr.Caches["newest_first_cache"]["newest_first_test"]
	assert.Equal(t, "98", checkpoint1.Value)

	// Simulate new records arriving
	apiState = append([]map[string]any{
		{"id": "102", "timestamp": "2024-01-01T10:02:00Z"},
		{"id": "101", "timestamp": "2024-01-01T10:01:00Z"},
	}, apiState...)

	// Second run: should fetch only new records (101, 102)
	input2, err := mgr.NewInput(conf)
	require.NoError(t, err)
	require.NoError(t, input2.Connect(ctx))

	var receivedIDs []string
	for {
		batch, ack, err := input2.ReadBatch(ctx)
		if err != nil {
			break
		}

		for _, msg := range batch {
			bytes, _ := msg.AsBytes()
			var record map[string]any
			require.NoError(t, json.Unmarshal(bytes, &record))
			receivedIDs = append(receivedIDs, record["id"].(string))
		}

		require.NoError(t, ack(ctx, nil))
	}
	require.NoError(t, input2.Close(ctx))

	// Should only get the 2 new records
	assert.Equal(t, []string{"102", "101"}, receivedIDs)

	// Checkpoint should now be updated to 101 (last_id from first page of this run)
	checkpoint2, _ := mgr.Caches["newest_first_cache"]["newest_first_test"]
	assert.Equal(t, "101", checkpoint2.Value)
}
