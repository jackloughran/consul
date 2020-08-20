package agent

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/hashicorp/consul/agent/dibs"
	"github.com/hashicorp/consul/agent/structs"
)

const (
	beServicesPrefix  = "be/services"
	currentVersionKey = beServicesPrefix + "/current_version.json"
	schemaFileName    = "schema.json"
)

type requestType int

const (
	requestTypeJsonConfigs = iota
	requestTypeConfigFiles
	requestTypeSingleConfigFile
)

var configsCache = make(map[string]map[string]map[string]string)

const maxCachedConfigs = 3

var configsCacheVersions []string

type DibsConfigsResponse struct {
	Buckets []string
	Configs map[string]interface{}
}

type DibsConfigFilesResponse struct {
	FileNames      []string
	CurrentVersion string
}

func (s *HTTPServer) DibsJsonConfigs(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	return s.doDibsConfigs(resp, req, requestTypeJsonConfigs)
}

func (s *HTTPServer) DibsConfigFiles(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	return s.doDibsConfigs(resp, req, requestTypeConfigFiles)
}

func (s *HTTPServer) DibsSingleConfigFile(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	return s.doDibsConfigs(resp, req, requestTypeSingleConfigFile)
}

func (s *HTTPServer) doDibsConfigs(resp http.ResponseWriter, req *http.Request, requestType requestType) (interface{}, error) {
	configBucket := req.URL.Query().Get("configBucket")
	if configBucket == "" {
		return nil, fmt.Errorf("must pass configBucket param")
	}

	service := req.URL.Query().Get("service")
	if service == "" {
		return nil, fmt.Errorf("must pass service param")
	}

	isLocal := req.URL.Query().Get("local") == "true"

	currentVersion := req.URL.Query().Get("currentVersion")
	if currentVersion == "" {
		if requestType == requestTypeSingleConfigFile {
			return nil, fmt.Errorf("must pass currentVersion param")
		}

		var err error
		currentVersion, err = s.getValue(currentVersionKey)
		if err != nil {
			return nil, err
		}
	}

	schema, err := s.getSchema(currentVersion)
	if err != nil {
		return nil, err
	}

	buckets, err := dibs.GetAllConfigBuckets(configBucket, schema, service, isLocal)
	if err != nil {
		return nil, err
	}

	configs, err := s.getConfigs(currentVersion, service, buckets)
	if err != nil {
		return nil, err
	}

	switch requestType {
	case requestTypeJsonConfigs:
		return DibsConfigsResponse{
			Buckets: buckets,
			Configs: dibs.GroupConfigs(configs),
		}, nil

	case requestTypeConfigFiles:
		return DibsConfigFilesResponse{
			FileNames:      dibs.GetConfigFileNames(configs),
			CurrentVersion: currentVersion,
		}, nil

	case requestTypeSingleConfigFile:
		fileName := req.URL.Query().Get("fileName")
		if fileName == "" {
			return nil, fmt.Errorf("must provide fileName query param")
		}

		s, err := dibs.GetSingleConfigFile(fileName, configs)
		if err != nil {
			return nil, err
		}

		if s == "" {
			resp.WriteHeader(http.StatusNotFound)
		}

		fmt.Fprint(resp, s)
		return nil, nil
	}

	return nil, fmt.Errorf("fell through switch somehow")
}

func (s *HTTPServer) getSchema(currentVersion string) (map[string]map[string]interface{}, error) {
	schemaJSON, err := s.getValue(beServicesPrefix + "/" + currentVersion + "/" + schemaFileName)
	if err != nil {
		return nil, err
	}

	var schema map[string]map[string]interface{}
	json.Unmarshal([]byte(schemaJSON), &schema)

	return schema, nil
}

func (s *HTTPServer) getConfigs(currentVersion, service string, buckets []string) (map[string]string, error) {
	configsByService := configsCache[currentVersion]
	var configs map[string]string
	if configsByService != nil {
		configs = configsByService[service]
	}

	if configsByService == nil || configs == nil {
		allConfigs, err := s.getValues(beServicesPrefix + "/" + currentVersion + "/")
		if err != nil {
			return nil, err
		}

		configs, err = dibs.GetConfigs(buckets, allConfigs)
		if err != nil {
			return nil, err
		}

		if configsCache[currentVersion] == nil {
			configsCache[currentVersion] = make(map[string]map[string]string)
		}

		configsCache[currentVersion][service] = configs
		configsCacheVersions = append(configsCacheVersions, currentVersion)
		if len(configsCacheVersions) > maxCachedConfigs {
			configsCacheVersions = configsCacheVersions[len(configsCacheVersions)-maxCachedConfigs:]
		}
	}

	return configs, nil
}

func (s *HTTPServer) getValue(key string) (string, error) {
	args := structs.KeyRequest{
		Datacenter: "dibs-consul",
		Key:        key,
	}

	var out structs.IndexedDirEntries
	if err := s.agent.RPC("KVS.Get", &args, &out); err != nil {
		return "", err
	}

	return string(out.Entries[0].Value), nil
}

func (s *HTTPServer) getValues(prefix string) (map[string]string, error) {
	args := structs.KeyRequest{
		Datacenter: "dibs-consul",
		Key:        prefix,
	}

	var out structs.IndexedDirEntries
	if err := s.agent.RPC("KVS.List", &args, &out); err != nil {
		return nil, err
	}

	values := make(map[string]string)
	for _, entry := range out.Entries {
		values[entry.Key] = string(entry.Value)
	}

	return values, nil
}
