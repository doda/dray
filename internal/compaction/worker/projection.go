package worker

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/dray-io/dray/internal/projection"
)

type recordProjector struct {
	fields []projectionField
}

type projectionField struct {
	name string
	path []string
	typ  projection.FieldType
}

func newRecordProjector(fields []projection.FieldSpec) *recordProjector {
	if len(fields) == 0 {
		return nil
	}
	out := &recordProjector{
		fields: make([]projectionField, 0, len(fields)),
	}
	for _, field := range fields {
		if field.Name == "" {
			continue
		}
		path := strings.Split(field.Path, ".")
		out.fields = append(out.fields, projectionField{
			name: field.Name,
			path: path,
			typ:  field.Type,
		})
	}
	if len(out.fields) == 0 {
		return nil
	}
	return out
}

func projectValue(value []byte, fallbackTimestamp int64, projector *recordProjector) map[string]any {
	if projector == nil {
		return nil
	}
	return projector.projectJSON(value, fallbackTimestamp)
}

func (p *recordProjector) projectJSON(value []byte, fallbackTimestamp int64) map[string]any {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}

	var payload any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil
	}

	out := make(map[string]any)
	for _, field := range p.fields {
		raw, ok := lookupPath(payload, field.path)
		if !ok {
			continue
		}
		if projected, ok := coerceProjectionValue(raw, field.typ, fallbackTimestamp); ok {
			out[field.name] = projected
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func lookupPath(value any, path []string) (any, bool) {
	current := value
	for _, part := range path {
		if part == "" {
			continue
		}
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(typed) {
				return nil, false
			}
			current = typed[idx]
		default:
			return nil, false
		}
	}
	if current == nil {
		return nil, false
	}
	return current, true
}

func coerceProjectionValue(raw any, fieldType projection.FieldType, fallbackTimestamp int64) (any, bool) {
	switch fieldType {
	case projection.FieldTypeString:
		return coerceString(raw)
	case projection.FieldTypeInt32:
		return coerceInt32(raw)
	case projection.FieldTypeInt64:
		return coerceInt64(raw)
	case projection.FieldTypeBool:
		return coerceBool(raw)
	case projection.FieldTypeTimestampMs:
		return coerceTimestampMs(raw, fallbackTimestamp)
	case projection.FieldTypeStringList:
		return coerceStringList(raw)
	case projection.FieldTypeInt64List:
		return coerceInt64List(raw)
	case projection.FieldTypeBoolList:
		return coerceBoolList(raw)
	default:
		return nil, false
	}
}

func coerceString(raw any) (string, bool) {
	switch value := raw.(type) {
	case string:
		return value, true
	default:
		return "", false
	}
}

func coerceInt64(raw any) (int64, bool) {
	switch value := raw.(type) {
	case json.Number:
		if parsed, err := value.Int64(); err == nil {
			return parsed, true
		}
		if parsed, err := value.Float64(); err == nil {
			return int64(parsed), true
		}
		return 0, false
	case float64:
		return int64(value), true
	case int64:
		return value, true
	case int32:
		return int64(value), true
	case int:
		return int64(value), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func coerceInt32(raw any) (int32, bool) {
	parsed, ok := coerceInt64(raw)
	if !ok {
		return 0, false
	}
	if parsed > int64(int32(^uint32(0)>>1)) || parsed < int64(-int32(^uint32(0)>>1)-1) {
		return 0, false
	}
	return int32(parsed), true
}

func coerceBool(raw any) (bool, bool) {
	switch value := raw.(type) {
	case bool:
		return value, true
	case string:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return false, false
		}
		return parsed, true
	default:
		return false, false
	}
}

func coerceTimestampMs(raw any, fallbackTimestamp int64) (int64, bool) {
	switch value := raw.(type) {
	case string:
		parsed, ok := parseTimestamp(value)
		if !ok {
			if fallbackTimestamp > 0 {
				return fallbackTimestamp, true
			}
			return 0, false
		}
		return parsed.UnixMilli(), true
	default:
		parsed, ok := coerceInt64(raw)
		if !ok {
			if fallbackTimestamp > 0 {
				return fallbackTimestamp, true
			}
			return 0, false
		}
		return parsed, true
	}
}

func parseTimestamp(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, true
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, true
	}
	return time.Time{}, false
}

func coerceStringList(raw any) ([]string, bool) {
	switch value := raw.(type) {
	case []string:
		return value, true
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func coerceInt64List(raw any) ([]int64, bool) {
	switch value := raw.(type) {
	case []int64:
		return value, true
	case []any:
		out := make([]int64, 0, len(value))
		for _, item := range value {
			if parsed, ok := coerceInt64(item); ok {
				out = append(out, parsed)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func coerceBoolList(raw any) ([]bool, bool) {
	switch value := raw.(type) {
	case []bool:
		return value, true
	case []any:
		out := make([]bool, 0, len(value))
		for _, item := range value {
			if parsed, ok := coerceBool(item); ok {
				out = append(out, parsed)
			}
		}
		return out, true
	default:
		return nil, false
	}
}
