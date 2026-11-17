package io

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
)

// Bento's input read loop retries any non-EndOfInput read error forever (its read backoff never
// gives up), so a permanent failure used to keep the whole process alive until it was killed (a
// Lambda timeout, for the 401 case that triggered this). These tests run a real stream against a
// failing server and require that the stream ends on its own, quickly, having asked the server only
// as many times as one read cycle (1 + retries) allows.
func runStreamAgainst(t *testing.T, handler http.HandlerFunc, extra string) (hits *atomic.Int64, runErr error, took time.Duration) {
	t.Helper()
	hits = &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML(`level: OFF`))
	require.NoError(t, b.AddInputYAML(`
http_paginated:
  url: `+srv.URL+`/v1/items
  headers:
    Authorization: "Bearer test"
  retries: 2
  retry_period: 10ms
  max_retry_backoff: 20ms
  timeout: 2s
  pagination:
    type: cursor
    cursor:
      param_name: before
      next_cursor_field: last_id
      has_more_field: has_more
  response:
    data_field: data
    flatten: true
`+extra))
	require.NoError(t, b.AddConsumerFunc(func(context.Context, *service.Message) error { return nil }))
	s, err := b.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	runErr = s.Run(ctx)
	return hits, runErr, time.Since(start)
}

func TestHTTPPaginatedFailsFast_PermanentStatus(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			hits, err, took := runStreamAgainst(t, func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `{"error":"nope"}`, status)
			}, "")
			assert.NoError(t, err, "stream should end cleanly, not hit the 15s test deadline")
			assert.Less(t, took, 5*time.Second, "stream must end on its own")
			assert.EqualValues(t, 1, hits.Load(), "no status in backoff_on: one request, no retries")
		})
	}
}

func TestHTTPPaginatedFailsFast_RetriesExhausted(t *testing.T) {
	hits, err, took := runStreamAgainst(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}, "  backoff_on: [503]\n")
	assert.NoError(t, err)
	assert.Less(t, took, 5*time.Second)
	assert.EqualValues(t, 3, hits.Load(), "1 attempt + 2 retries, then stop (not another round)")
}

func TestHTTPPaginatedFailsFast_NonJSONBody(t *testing.T) {
	hits, err, took := runStreamAgainst(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<html>login</html>"))
	}, "")
	assert.NoError(t, err)
	assert.Less(t, took, 5*time.Second)
	assert.EqualValues(t, 1, hits.Load())
}

func TestHTTPPaginatedStillWorksOnSuccess(t *testing.T) {
	var page atomic.Int64
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if page.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"data":[{"id":"a"},{"id":"b"}],"has_more":true,"last_id":"b"}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"c"}],"has_more":false,"last_id":"c"}`))
	}))
	defer srv.Close()

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML(`level: OFF`))
	require.NoError(t, b.AddInputYAML(`
http_paginated:
  url: `+srv.URL+`/v1/items
  pagination:
    type: cursor
    cursor:
      param_name: before
      next_cursor_field: last_id
      has_more_field: has_more
  response:
    data_field: data
    flatten: true
`))
	require.NoError(t, b.AddConsumerFunc(func(context.Context, *service.Message) error { got.Add(1); return nil }))
	s, err := b.Build()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, s.Run(ctx))
	assert.EqualValues(t, 3, got.Load())
	assert.EqualValues(t, 2, page.Load())
}
