package config

import (
	"encoding/json"
	"net/url"
)

func (c *ClickHouse) MarshalJSON() ([]byte, error) {
	type ClickHouseRaw ClickHouse

	// make copy
	a := *c

	u, err := url.Parse(a.Url)
	if err != nil {
		a.Url = "<parse error>"
	} else {
		if _, isSet := u.User.Password(); isSet {
			u.User = url.UserPassword(u.User.Username(), "xxxxxx")
		}
		a.Url = u.String()
	}

	return json.Marshal((*ClickHouseRaw)(&a))
}
