package headers

import "net/http"

func GetHeaders(header *http.Header, keys []string) map[string]string {
	if len(keys) > 0 {
		headers := make(map[string]string)
		for _, key := range keys {
			value := header.Get(key)
			if len(value) > 0 {
				headers[key] = value
			}
		}
		return headers
	}
	return nil
}
