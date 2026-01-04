package projection

import "strings"

// Format describes how value projections are extracted.
type Format string

const (
	FormatJSON Format = "json"
)

// FieldType describes the projected column type.
type FieldType string

const (
	FieldTypeString      FieldType = "string"
	FieldTypeInt32       FieldType = "int32"
	FieldTypeInt64       FieldType = "int64"
	FieldTypeBool        FieldType = "bool"
	FieldTypeTimestampMs FieldType = "timestamp_ms"
	FieldTypeStringList  FieldType = "string_list"
	FieldTypeInt64List   FieldType = "int64_list"
	FieldTypeBoolList    FieldType = "bool_list"
)

// FieldSpec defines a projected column derived from the value payload.
type FieldSpec struct {
	Name string    `yaml:"name"`
	Path string    `yaml:"path"`
	Type FieldType `yaml:"type"`
}

// TopicProjection defines value projections for a topic.
type TopicProjection struct {
	Topic  string      `yaml:"topic"`
	Format Format      `yaml:"format"`
	Fields []FieldSpec `yaml:"fields"`
}

// Normalize trims fields and applies defaults (FormatJSON, Path=Name).
func Normalize(projections []TopicProjection) []TopicProjection {
	out := make([]TopicProjection, 0, len(projections))
	for _, proj := range projections {
		norm := TopicProjection{
			Topic:  strings.TrimSpace(proj.Topic),
			Format: Format(strings.ToLower(strings.TrimSpace(string(proj.Format)))),
		}
		if norm.Format == "" {
			norm.Format = FormatJSON
		}
		norm.Fields = make([]FieldSpec, 0, len(proj.Fields))
		for _, field := range proj.Fields {
			name := strings.TrimSpace(field.Name)
			path := strings.TrimSpace(field.Path)
			if path == "" {
				path = name
			}
			norm.Fields = append(norm.Fields, FieldSpec{
				Name: name,
				Path: path,
				Type: FieldType(strings.ToLower(strings.TrimSpace(string(field.Type)))),
			})
		}
		out = append(out, norm)
	}
	return out
}

// ByTopic returns a lookup map for projections keyed by topic.
func ByTopic(projections []TopicProjection) map[string]TopicProjection {
	out := make(map[string]TopicProjection, len(projections))
	for _, proj := range projections {
		if proj.Topic == "" {
			continue
		}
		out[proj.Topic] = proj
	}
	return out
}

// IsValidFormat returns true for supported projection formats.
func IsValidFormat(format Format) bool {
	switch format {
	case FormatJSON:
		return true
	default:
		return false
	}
}

// IsValidFieldType returns true for supported field types.
func IsValidFieldType(fieldType FieldType) bool {
	switch fieldType {
	case FieldTypeString,
		FieldTypeInt32,
		FieldTypeInt64,
		FieldTypeBool,
		FieldTypeTimestampMs,
		FieldTypeStringList,
		FieldTypeInt64List,
		FieldTypeBoolList:
		return true
	default:
		return false
	}
}

// IsListType reports whether the field type is a list.
func IsListType(fieldType FieldType) bool {
	switch fieldType {
	case FieldTypeStringList, FieldTypeInt64List, FieldTypeBoolList:
		return true
	default:
		return false
	}
}
