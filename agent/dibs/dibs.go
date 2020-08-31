package dibs

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
)

const (
	overrideFileName      = "override_file"
	globalBucketName      = "global"
	beServicesPrefixRegex = `be\/services`
	uatBucketName         = "uat"
	liveBucketPrefix      = "live_"
	uatBucketPrefix       = "uat_"
	localBucketName       = "local"
	filePrefix            = "FILES/"
)

type configType int

const (
	configTypeProperties = iota
	configTypeFile
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

func GetConfigs(buckets []string, configs map[string]string, tokensWithValues map[string]string) (map[string]string, error) {
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
				configsByFileName[fileName[1]] = tokenizeConfigValue(v, tokensWithValues)
			}
		}
	}

	return configsByFileName, nil
}

func tokenizeConfigValue(configValue string, tokensWithValues map[string]string) string {
	var newConfigValue = configValue
	for k, v := range tokensWithValues {
		tokenString := fmt.Sprintf("${%s}", k)
		newConfigValue = strings.ReplaceAll(newConfigValue, tokenString, v)
	}

	return newConfigValue
}

func GetBase64ConfigFiles(configsByFileName map[string]string) (map[string]string, error) {
	fileNamesSet := make(map[string]bool)

	for fileNameAndKey := range configsByFileName {
		fileName, _ := getFileName(fileNameAndKey)
		if !fileNamesSet[fileName] {
			fileNamesSet[fileName] = true
		}
	}

	files := make(map[string]string)
	for fileName, _ := range fileNamesSet {
		fileValue, err := getSingleConfigFile(fileName, configsByFileName)
		if err != nil {
			return nil, err
		}

		files[fileName] = base64.StdEncoding.EncodeToString([]byte(fileValue))
	}

	return files, nil
}

func getSingleConfigFile(fileName string, configsByFileName map[string]string) (string, error) {
	configsForThisFile := make(map[string]string)

	for fileNameAndKey, value := range configsByFileName {
		thisFileName, configType := getFileName(fileNameAndKey)

		if thisFileName == fileName {
			if configType == configTypeProperties {
				configsForThisFile[getConfigKey(fileNameAndKey)] = value
			} else {
				return value, nil
			}
		}
	}

	var b strings.Builder
	for key, value := range configsForThisFile {
		fmt.Fprintf(&b, "%s=%s\n", key, value)
	}

	return b.String(), nil
}

func GroupConfigs(configs map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range configs {
		fileName, configType := getFileName(k)
		if configType == configTypeProperties {
			var configFileEntry map[string]string
			if result[fileName] != nil {
				configFileEntry = result[fileName].(map[string]string)
			} else {
				configFileEntry = make(map[string]string)
			}

			configKey := getConfigKey(k)

			configFileEntry[configKey] = v
			result[fileName] = configFileEntry
		} else {
			result[fileName] = v
		}
	}

	return result
}

func getFileName(fileNameAndKey string) (string, configType) {
	if strings.HasPrefix(fileNameAndKey, filePrefix) {
		return fileNameAndKey[len(filePrefix):], configTypeFile
	}

	return strings.Split(fileNameAndKey, "#")[0], configTypeProperties
}

func getConfigKey(fileNameAndKey string) string {
	if strings.Contains(fileNameAndKey, "#") {
		return strings.Split(fileNameAndKey, "#")[1]
	}

	return fileNameAndKey
}
