package config

import (
	"encoding/json"
	"net/url"
)

func (c *ClickHouse) MarshalJSON() ([]byte, error) {
	type ClickHouseRaw ClickHouse

	// make copy
	a := *c

	u, err := url.Parse(a.URL)
	if err != nil {
		a.URL = "<parse error>"
	} else {
		if _, isSet := u.User.Password(); isSet {
			u.User = url.UserPassword(u.User.Username(), "xxxxxx")
		}
		a.URL = u.String()
	}

	return json.Marshal((*ClickHouseRaw)(&a))
}
