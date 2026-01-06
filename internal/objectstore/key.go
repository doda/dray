package objectstore

import "strings"

// NormalizeKey strips an s3://bucket/ prefix to return a bucket-relative key.
// Non-S3 paths are returned unchanged.
func NormalizeKey(path string) string {
	if strings.HasPrefix(path, "s3://") {
		trimmed := strings.TrimPrefix(path, "s3://")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
	}
	return path
}
