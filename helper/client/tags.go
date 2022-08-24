package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// TagsNames do  /tags/autoComplete/tags request with query like [tagPrefix];tag1=value1;tag2=~value*
// Valid formats are json
func TagsNames(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, error) {
	rTags := "/tags/autoComplete/tags"

	if format == FormatDefault {
		format = FormatJSON
	}

	queryParams := fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)

	switch format {
	case FormatJSON:
		break
	default:
		return queryParams, nil, ErrUnsupportedFormat
	}

	u, err := url.Parse(address + rTags)
	if err != nil {
		return queryParams, nil, err
	}

	var tagPrefix string
	var exprs []string

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 1 {
			return queryParams, nil, ErrInvalidQuery
		}

		exprs = make([]string, 0, len(args))
		for i, arg := range args {
			delim := strings.IndexRune(arg, '=')
			if i == 0 && delim == -1 {
				tagPrefix = arg
			} else if delim <= 0 {
				return queryParams, nil, errors.New("invalid expr: " + arg)
			} else {
				exprs = append(exprs, arg)
			}
		}
	}

	v := url.Values{
		"format": []string{format.String()},
	}
	if len(exprs) > 0 {
		v["expr"] = exprs
	}
	if tagPrefix != "" {
		v["tagPrefix"] = []string{tagPrefix}
	}
	if from > 0 {
		v["from"] = []string{strconv.FormatInt(from, 10)}
	}
	if until > 0 {
		v["until"] = []string{strconv.FormatInt(until, 10)}
	}
	if limit > 0 {
		v["limit"] = []string{strconv.FormatUint(limit, 10)}
	}

	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return queryParams, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return queryParams, nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return u.RawQuery, nil, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, fmt.Errorf("error with %d: %s", resp.StatusCode, string(b))
	}

	var values []string
	err = json.Unmarshal(b, &values)
	if err != nil {
		return queryParams, nil, errors.New(err.Error() + ": " + string(b))
	}

	return queryParams, values, nil
}

// TagsValues do  /tags/autoComplete/values request with query like searchTag[=valuePrefix];tag1=value1;tag2=~value*
// Valid formats are json
func TagsValues(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, error) {
	rTags := "/tags/autoComplete/values"

	if format == FormatDefault {
		format = FormatJSON
	}

	queryParams := fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)

	switch format {
	case FormatJSON:
		break
	default:
		return queryParams, nil, ErrUnsupportedFormat
	}

	u, err := url.Parse(address + rTags)
	if err != nil {
		return queryParams, nil, err
	}

	var (
		tags        []string
		valuePrefix string
		exprs       []string
	)

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 2 {
			return queryParams, nil, ErrInvalidQuery
		}

		vals := strings.Split(args[0], "=")
		tags = []string{vals[0]}
		if len(vals) > 2 {
			return queryParams, nil, errors.New("invalid tag: " + args[0])
		} else if len(vals) == 2 {
			valuePrefix = vals[1]
		}

		exprs = make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			expr := args[i]
			if strings.IndexRune(expr, '=') <= 0 {
				return queryParams, nil, errors.New("invalid expr: " + expr)
			}
			exprs = append(exprs, expr)
		}
	}

	v := url.Values{
		"format": []string{format.String()},
	}
	if len(exprs) > 0 {
		v["expr"] = exprs
	}
	if len(tags) > 0 {
		v["tag"] = tags
	}
	if valuePrefix != "" {
		v["valuePrefix"] = []string{valuePrefix}
	}
	if from > 0 {
		v["from"] = []string{strconv.FormatInt(from, 10)}
	}
	if until > 0 {
		v["until"] = []string{strconv.FormatInt(until, 10)}
	}
	if limit > 0 {
		v["limit"] = []string{strconv.FormatUint(limit, 10)}
	}

	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return queryParams, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return u.RawQuery, nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return queryParams, nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return queryParams, nil, nil
	} else if resp.StatusCode != http.StatusOK {
		return queryParams, nil, fmt.Errorf("error with %d: %s", resp.StatusCode, string(b))
	}

	var values []string
	err = json.Unmarshal(b, &values)
	if err != nil {
		return queryParams, nil, errors.New(err.Error() + ": " + string(b))
	}

	return queryParams, values, nil
}
