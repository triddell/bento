package aws

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression test for the 2026-11-10 river-w outage. The streaming output builds the Parquet schema
// with parquet.SchemaOf(), and parquet-go rejects a pointer to anything but time.Time under the
// "timestamp"/"date" struct tags. Optional TIMESTAMP_MILLIS / DATE columns therefore panicked at
// output start-up whenever the struct passed to SchemaOf had optionals as pointers. The schema must
// be derived from a non-pointer struct (optionals as struct tags), while the rows keep pointers so
// that NULLs survive; this mirrors what the upstream parquet_encode processor does.
const optionalTimeSchemaYAML = `
bucket: test-bucket
path: 'data.parquet'
schema:
  - name: id
    type: INT64
  - name: time
    type: TIMESTAMP_MILLIS
  - name: processed_time
    type: TIMESTAMP_MILLIS
    optional: true
  - name: o_date
    type: DATE
    optional: true
  - name: note
    type: UTF8
    optional: true
  - name: metadata
    optional: true
    fields:
      - name: version
        type: UTF8
        optional: true
      - name: finished_at
        type: TIMESTAMP_MILLIS
        optional: true
`

func TestS3ParquetStreamOutput_OptionalTimeColumns(t *testing.T) {
	parsed, err := s3ParquetStreamOutputSpec().ParseYAML(optionalTimeSchemaYAML, nil)
	require.NoError(t, err)

	// Must not panic while the schema is built.
	conf, err := s3ParquetStreamConfigFromParsed(parsed)
	require.NoError(t, err)
	require.NotNil(t, conf.Schema)

	events := []map[string]any{
		{ // everything populated
			"id": int64(1), "time": int64(1790162333035),
			"processed_time": int64(1790162334000), "o_date": int32(20719), "note": "n",
			"metadata": map[string]any{"version": "1.0", "finished_at": int64(1790162335000)},
		},
		{ // optionals missing: must come back as NULL, not 0
			"id": int64(2), "time": int64(1790162333036),
		},
		{ // optional parent present, optional children missing
			"id": int64(3), "time": int64(1790162333037),
			"metadata": map[string]any{},
		},
	}

	var buf bytes.Buffer
	pw := parquet.NewGenericWriter[any](&buf, conf.Schema)
	for _, e := range events {
		v := newMessage(t, conf.MessageType, e)
		_, err := pw.Write([]any{v})
		require.NoError(t, err)
	}
	require.NoError(t, pw.Close())

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	assert.Equal(t, int64(3), f.NumRows())

	// Leaf column order: id, time, processed_time, o_date, note, metadata.version, metadata.finished_at
	cols := map[string][]parquet.Value{}
	for _, name := range []string{"id", "time", "processed_time", "o_date", "note"} {
		cols[name] = readColumn(t, f, name)
	}
	cols["metadata.version"] = readColumn(t, f, "metadata", "version")
	cols["metadata.finished_at"] = readColumn(t, f, "metadata", "finished_at")

	require.Len(t, cols["processed_time"], 3)
	assert.False(t, cols["processed_time"][0].IsNull())
	assert.Equal(t, int64(1790162334000), cols["processed_time"][0].Int64())
	assert.True(t, cols["processed_time"][1].IsNull(), "missing optional timestamp must be NULL")
	assert.True(t, cols["processed_time"][2].IsNull())

	assert.False(t, cols["o_date"][0].IsNull())
	assert.Equal(t, int32(20719), cols["o_date"][0].Int32())
	assert.True(t, cols["o_date"][1].IsNull(), "missing optional date must be NULL")

	assert.Equal(t, int64(1790162335000), cols["metadata.finished_at"][0].Int64())
	assert.True(t, cols["metadata.finished_at"][1].IsNull(), "absent parent -> NULL")
	assert.True(t, cols["metadata.finished_at"][2].IsNull(), "present parent, absent child -> NULL")

	// The logical type annotation must survive: TIMESTAMP(MILLIS) on both the required and optional column.
	for _, name := range []string{"time", "processed_time"} {
		leaf, ok := conf.Schema.Lookup(name)
		require.True(t, ok, name)
		assert.Equal(t, "TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)", leaf.Node.Type().LogicalType().String(), name)
		assert.Equal(t, name == "processed_time", leaf.Node.Optional(), name)
	}
}

func readColumn(t *testing.T, f *parquet.File, path ...string) []parquet.Value {
	t.Helper()
	leaf, ok := f.Schema().Lookup(path...)
	require.True(t, ok, "column %v", path)
	var out []parquet.Value
	for _, rg := range f.RowGroups() {
		pages := rg.ColumnChunks()[leaf.ColumnIndex].Pages()
		for {
			pg, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			vals := make([]parquet.Value, pg.NumValues())
			n, err := pg.Values().ReadValues(vals)
			if err != nil && err != io.EOF {
				require.NoError(t, err)
			}
			out = append(out, vals[:n]...)
			parquet.Release(pg)
		}
		require.NoError(t, pages.Close())
	}
	return out
}

func newMessage(t *testing.T, typ reflect.Type, event map[string]any) any {
	t.Helper()
	v := reflect.New(typ)
	require.NoError(t, mapToStruct(event, v.Interface()))
	return v.Interface()
}
