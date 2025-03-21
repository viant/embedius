package meta

func GetString(metadata map[string]any, key string) string {
	if value, ok := metadata[key]; ok {
		text, _ := value.(string)
		return text
	}
	return ""
}
