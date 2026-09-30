package store

import (
	"bytes"
	"encoding/base64"
)

// EncodeKey produces the Pebble key: namespace + 0x00 + key.
func EncodeKey(namespace, key string) []byte {
	buf := make([]byte, len(namespace)+1+len(key))
	copy(buf, namespace)
	buf[len(namespace)] = 0x00
	copy(buf[len(namespace)+1:], key)
	return buf
}

// DecodeKey splits a Pebble key back into (namespace, key).
func DecodeKey(raw []byte) (namespace, key string) {
	idx := bytes.IndexByte(raw, 0x00)
	if idx == -1 {
		return string(raw), ""
	}
	return string(raw[:idx]), string(raw[idx+1:])
}

// NamespacePrefix returns the prefix for scanning all keys in a namespace.
func NamespacePrefix(namespace string) []byte {
	buf := make([]byte, len(namespace)+1)
	copy(buf, namespace)
	buf[len(namespace)] = 0x00
	return buf
}

// NamespacePrefixWithKey returns the prefix for scanning keys with a given key prefix in a namespace.
func NamespacePrefixWithKey(namespace, keyPrefix string) []byte {
	buf := make([]byte, len(namespace)+1+len(keyPrefix))
	copy(buf, namespace)
	buf[len(namespace)] = 0x00
	copy(buf[len(namespace)+1:], keyPrefix)
	return buf
}

// prefixUpperBound returns the lexicographic successor of a prefix for iteration bounds.
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper
		}
	}
	return nil // prefix was all 0xFF
}

func encodePageToken(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

func decodePageToken(token string) []byte {
	data, _ := base64.RawURLEncoding.DecodeString(token)
	return data
}
