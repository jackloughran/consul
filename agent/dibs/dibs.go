package dibs

const (
	overrideFileName = "override_file"
	globalBucketName = "global"
)

func GetAllConfigBuckets(configBucket string, schema map[string]map[string]interface{}, isLocal bool) ([]string, error) {
	allConfigBuckets := []string{configBucket}

	var currentBucket = configBucket

	for currentBucket != globalBucketName {
		schemaEntry := schema[currentBucket]
		parents := schemaEntry["parents"].([]interface{})
		parent := parents[0].(string)

		allConfigBuckets = append(allConfigBuckets, parent)

		currentBucket = parent
	}

	return allConfigBuckets, nil
}
