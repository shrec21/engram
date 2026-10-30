package store

import (
	"bytes"
	"encoding/base64"
)

// SystemKeyPrefix is the prefix for all system keys (dedup, etc.).
// System keys sort after all user keys since 0xFF is the highest byte.
var SystemKeyPrefix = []byte{0xFF}

// DedupKeyPrefix is the prefix for dedup table entries.
// Layout: 0xFF 0x00 "dedup" 0x00 <client_id>
var DedupKeyPrefix = []byte{0xFF, 0x00, 'd', 'e', 'd', 'u', 'p', 0x00}

// DecayPolicyKeyPrefix is the prefix for decay policy entries.
// Layout: 0xFF 0x01 "decay" 0x00 <namespace>
var DecayPolicyKeyPrefix = []byte{0xFF, 0x01, 'd', 'e', 'c', 'a', 'y', 0x00}

// ShardMapKey is the fixed key for the shard map.
// Layout: 0xFF 0x02 "shardmap"
var ShardMapKey = []byte{0xFF, 0x02, 's', 'h', 'a', 'r', 'd', 'm', 'a', 'p'}

// TokenKeyPrefix is the prefix for auth token entries.
// Layout: 0xFF 0x03 "token" 0x00 <token_hash>
var TokenKeyPrefix = []byte{0xFF, 0x03, 't', 'o', 'k', 'e', 'n', 0x00}

// EncodeDecayPolicyKey returns the Pebble key for a decay policy entry.
func EncodeDecayPolicyKey(namespace string) []byte {
	buf := make([]byte, len(DecayPolicyKeyPrefix)+len(namespace))
	copy(buf, DecayPolicyKeyPrefix)
	copy(buf[len(DecayPolicyKeyPrefix):], namespace)
	return buf
}

// DecodeDecayPolicyKey extracts the namespace from a decay policy key. Returns "" if invalid.
func DecodeDecayPolicyKey(raw []byte) string {
	if !bytes.HasPrefix(raw, DecayPolicyKeyPrefix) {
		return ""
	}
	return string(raw[len(DecayPolicyKeyPrefix):])
}

// EncodeDedupKey returns the Pebble key for a dedup entry.
func EncodeDedupKey(clientID string) []byte {
	buf := make([]byte, len(DedupKeyPrefix)+len(clientID))
	copy(buf, DedupKeyPrefix)
	copy(buf[len(DedupKeyPrefix):], clientID)
	return buf
}

// DecodeDedupKey extracts the client_id from a dedup key. Returns "" if invalid.
func DecodeDedupKey(raw []byte) string {
	if !bytes.HasPrefix(raw, DedupKeyPrefix) {
		return ""
	}
	return string(raw[len(DedupKeyPrefix):])
}

// EncodeTokenKey returns the Pebble key for an auth token entry.
func EncodeTokenKey(tokenHash string) []byte {
	buf := make([]byte, len(TokenKeyPrefix)+len(tokenHash))
	copy(buf, TokenKeyPrefix)
	copy(buf[len(TokenKeyPrefix):], tokenHash)
	return buf
}

// DecodeTokenKey extracts the token_hash from a token key. Returns "" if invalid.
func DecodeTokenKey(raw []byte) string {
	if !bytes.HasPrefix(raw, TokenKeyPrefix) {
		return ""
	}
	return string(raw[len(TokenKeyPrefix):])
}

// IsSystemKey returns true if the key is in the system key range.
func IsSystemKey(key []byte) bool {
	return len(key) > 0 && key[0] == 0xFF
}

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
