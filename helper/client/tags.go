package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/msaf1980/go-stringutils"
)

// TagsNames do  /tags/autoComplete/tags request with query like [tagPrefix];tag1=value1;tag2=~value*
// Valid formats are json
func TagsNames(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, http.Header, error) {
	rTags := "/tags/autoComplete/tags"

	if format == FormatDefault {
		format = FormatJSON
	}

	var queryParams string

	switch format {
	case FormatJSON:
		break
	default:
		queryParams = fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)
		return queryParams, nil, nil, ErrUnsupportedFormat
	}

	u, err := url.Parse(address + rTags)
	if err != nil {
		return queryParams, nil, nil, err
	}

	var tagPrefix string
	var exprs []string

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 1 {
			return queryParams, nil, nil, ErrInvalidQuery
		}

		exprs = make([]string, 0, len(args))
		for i, arg := range args {
			delim := strings.IndexRune(arg, '=')
			if i == 0 && delim == -1 {
				tagPrefix = arg
			} else if delim <= 0 {
				return queryParams, nil, nil, errors.New("invalid expr: " + arg)
			} else {
				exprs = append(exprs, arg)
			}
		}
	}

	v := make([]string, 0, 2+len(exprs))
	var rawQuery stringutils.Builder
	rawQuery.Grow(128)

	v = append(v, "format="+format.String())
	rawQuery.WriteString("format=")
	rawQuery.WriteString(url.QueryEscape(format.String()))

	if tagPrefix != "" {
		v = append(v, "tagPrefix="+tagPrefix)
		rawQuery.WriteString("&tagPrefix=")
		rawQuery.WriteString(url.QueryEscape(tagPrefix))
	}
	for _, expr := range exprs {
		v = append(v, "expr="+expr)
		rawQuery.WriteString("&expr=")
		rawQuery.WriteString(url.QueryEscape(expr))
	}

	if from > 0 {
		fromStr := strconv.FormatInt(from, 10)
		v = append(v, "from="+fromStr)
		rawQuery.WriteString("&from=")
		rawQuery.WriteString(fromStr)
	}
	if until > 0 {
		untilStr := strconv.FormatInt(until, 10)
		v = append(v, "until="+untilStr)
		rawQuery.WriteString("&until=")
		rawQuery.WriteString(untilStr)
	}
	if limit > 0 {
		limitStr := strconv.FormatUint(limit, 10)
		v = append(v, "limit="+limitStr)
		rawQuery.WriteString("&limit=")
		rawQuery.WriteString(limitStr)
	}

	queryParams = fmt.Sprintf("%s %q", rTags, v)

	u.RawQuery = rawQuery.String()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return queryParams, nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return queryParams, nil, nil, err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return u.RawQuery, nil, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var values []string
	err = json.Unmarshal(b, &values)
	if err != nil {
		return queryParams, nil, resp.Header, errors.New(err.Error() + ": " + string(b))
	}

	return queryParams, values, resp.Header, nil
}

// TagsValues do  /tags/autoComplete/values request with query like searchTag[=valuePrefix];tag1=value1;tag2=~value*
// Valid formats are json
func TagsValues(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, http.Header, error) {
	rTags := "/tags/autoComplete/values"

	if format == FormatDefault {
		format = FormatJSON
	}

	var queryParams string

	switch format {
	case FormatJSON:
		break
	default:
		queryParams = fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)
		return queryParams, nil, nil, ErrUnsupportedFormat
	}

	u, err := url.Parse(address + rTags)
	if err != nil {
		return queryParams, nil, nil, err
	}

	var (
		tag         string
		valuePrefix string
		exprs       []string
	)

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 2 {
			return queryParams, nil, nil, ErrInvalidQuery
		}

		vals := strings.Split(args[0], "=")
		tag = vals[0]
		if len(vals) > 2 {
			return queryParams, nil, nil, errors.New("invalid tag: " + args[0])
		} else if len(vals) == 2 {
			valuePrefix = vals[1]
		}

		exprs = make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			expr := args[i]
			if strings.IndexRune(expr, '=') <= 0 {
				return queryParams, nil, nil, errors.New("invalid expr: " + expr)
			}
			exprs = append(exprs, expr)
		}
	}

	v := make([]string, 0, 2+len(exprs))
	var rawQuery stringutils.Builder
	rawQuery.Grow(128)

	v = append(v, "format="+format.String())
	rawQuery.WriteString("format=")
	rawQuery.WriteString(url.QueryEscape(format.String()))

	if tag != "" {
		v = append(v, "tag="+tag)
		rawQuery.WriteString("&tag=")
		rawQuery.WriteString(url.QueryEscape(tag))
	}
	if valuePrefix != "" {
		v = append(v, "valuePrefix="+valuePrefix)
		rawQuery.WriteString("&valuePrefix=")
		rawQuery.WriteString(url.QueryEscape(valuePrefix))
	}
	for _, expr := range exprs {
		v = append(v, "expr="+expr)
		rawQuery.WriteString("&expr=")
		rawQuery.WriteString(url.QueryEscape(expr))
	}

	if from > 0 {
		fromStr := strconv.FormatInt(from, 10)
		v = append(v, "from="+fromStr)
		rawQuery.WriteString("&from=")
		rawQuery.WriteString(fromStr)
	}
	if until > 0 {
		untilStr := strconv.FormatInt(until, 10)
		v = append(v, "until="+untilStr)
		rawQuery.WriteString("&until=")
		rawQuery.WriteString(untilStr)
	}
	if limit > 0 {
		limitStr := strconv.FormatUint(limit, 10)
		v = append(v, "limit="+limitStr)
		rawQuery.WriteString("&limit=")
		rawQuery.WriteString(limitStr)
	}

	queryParams = fmt.Sprintf("%s %q", rTags, v)

	u.RawQuery = rawQuery.String()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return queryParams, nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return u.RawQuery, nil, nil, err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return queryParams, nil, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var values []string
	err = json.Unmarshal(b, &values)
	if err != nil {
		return queryParams, nil, resp.Header, errors.New(err.Error() + ": " + string(b))
	}

	return queryParams, values, resp.Header, nil
}
