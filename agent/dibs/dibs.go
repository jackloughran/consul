package dibs

import (
	"regexp"
)

const (
	overrideFileName      = "override_file"
	globalBucketName      = "global"
	beServicesPrefixRegex = `be\/services`
	uatBucketName         = "uat"
	liveBucketPrefix      = "live_"
	uatBucketPrefix       = "uat_"
	localBucketName       = "local"
)

func GetAllConfigBuckets(configBucket string, schema map[string]map[string]interface{}, service string, isLocal bool) ([]string, error) {
	var allConfigBuckets []string
	if isLocal {
		allConfigBuckets = []string{localBucketName}
	} else {
		allConfigBuckets = []string{}
	}

	allConfigBuckets = addConfigBucket(configBucket, service, allConfigBuckets)

	var currentBucket = configBucket

	for currentBucket != globalBucketName {
		if currentBucket == uatBucketName {
			currentBucket = liveBucketPrefix + configBucket[len(uatBucketPrefix):]
		} else {
			schemaEntry := schema[currentBucket]
			parents := schemaEntry["parents"].([]interface{})
			parent := parents[0].(string)

			currentBucket = parent
		}

		allConfigBuckets = addConfigBucket(currentBucket, service, allConfigBuckets)
	}

	return allConfigBuckets, nil
}

func addConfigBucket(bucket, service string, allConfigBuckets []string) []string {
	return append(allConfigBuckets, bucket+"#"+service, bucket)
}

func GetConfigs(buckets []string, configs map[string]string) (map[string]string, error) {
	configsByFileName := make(map[string]string)

	for i := len(buckets) - 1; i >= 0; i = i - 1 {
		bucket := buckets[i]

		r, err := regexp.Compile(beServicesPrefixRegex + `\/[^\/]*\/` + bucket + `\/(.*)`)
		if err != nil {
			return nil, err
		}

		for k, v := range configs {
			fileName := r.FindStringSubmatch(k)
			if fileName != nil && fileName[1] != "" {
				configsByFileName[fileName[1]] = v
			}
		}
	}

	return configsByFileName, nil
}
