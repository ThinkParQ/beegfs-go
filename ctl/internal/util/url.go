package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/url"
)

func URLEncodeSignMap(m map[string]string, key string) (string, error) {
	var u url.URL
	q := u.Query()
	for k, v := range m {
		q.Add(k, v)
	}
	tosign := []byte(q.Encode())

	mac := hmac.New(sha256.New, []byte(m[key]))
	_, err := mac.Write(tosign)
	if err != nil {
		return "", err
	}

	macB64 := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	q.Add("mac", macB64)
	out := q.Encode()

	return out, nil
}
