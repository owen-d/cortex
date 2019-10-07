package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cortexproject/cortex/pkg/querier/astmapper"
	"github.com/cortexproject/cortex/pkg/querier/frontend"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// pulled from prometheus api pkg
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	// pulled from prometheus api pkg
	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

type ParallelQueryManager struct {
	queryEngine *promql.Engine
	queryable   storage.Queryable
	astMapper   astmapper.ASTMapper
	passthrough http.RoundTripper
	frontend    *frontend.Frontend
}

func NewParallelQueryManager(
	engine *promql.Engine,
	queryable storage.Queryable,
	astMapper astmapper.ASTMapper,
	frontend *frontend.Frontend,
) *ParallelQueryManager {
	return &ParallelQueryManager{
		queryEngine: engine,
		queryable:   queryable,
		astMapper:   astMapper,
		frontend:    frontend,
	}

}

// impls http.RoundTripper
func (m *ParallelQueryManager) RoundTrip(r *http.Request) (*http.Response, error) {
	var ctx context.Context
	var query promql.Query
	var err error

	if strings.HasSuffix(r.URL.Path, "/query_range") {
		ctx, query, err = m.parseRangeQuery(r)
	} else if strings.HasSuffix(r.URL.Path, "/query") {
		ctx, query, err = m.parseInstantQuery(r)
	} else if m.passthrough == nil {
		return nil, errors.Errorf("no passthrough http.Roundtripper")
	} else {
		return m.passthrough.RoundTrip(r)
	}

	if err != nil {
		return nil, err
	}

	// build query & exec

	res := query.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}

	encoded, err := json.Marshal(res)

	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewReader(encoded)),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
	}

	return httpResp, nil
}

// lifted from prometheus api internals
func (m *ParallelQueryManager) parseInstantQuery(r *http.Request) (context.Context, promql.Query, error) {
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "invalid parameter 'time'")
		}
	} else {
		ts = m.now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "invalid parameter 'timeout'")
		}

		ctx, _ = context.WithTimeout(ctx, timeout)
	}

	qry, err := m.queryEngine.NewInstantQuery(m.queryable, r.FormValue("query"), ts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid parameter 'query'")
	}

	return ctx, qry, nil
}

// lifted from prometheus api internals
func (m *ParallelQueryManager) parseRangeQuery(r *http.Request) (ctx context.Context, query promql.Query, err error) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid parameter 'start'")
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid parameter 'end'")
	}
	if end.Before(start) {
		return nil, nil, errors.New("end timestamp must not be before start time")
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "invalid parameter 'step'")
	}

	if step <= 0 {
		return nil, nil, errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		return nil, nil, errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
	}

	ctx = r.Context()
	if to := r.FormValue("timeout"); to != "" {
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, nil, errors.Wrap(err, "invalid parameter 'timeout'")
		}

		ctx, _ = context.WithTimeout(ctx, timeout)
	}

	qry, err := m.queryEngine.NewRangeQuery(m.queryable, r.FormValue("query"), start, end, step)
	if err != nil {
		return nil, nil, errors.Errorf("Bad query")
	}

	return ctx, qry, nil
}

/*
http.Request -> split by instant | Range -> validate -> parse -> astmapper -> query.exec -> eval'd in frontend
-> embedded queries are requested in queryable -> map to request and enqueue
-> collect all -> merge

Querier:

*/

// copied from prometheus api pkg
func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

// copied from prometheus api pkg
func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}
