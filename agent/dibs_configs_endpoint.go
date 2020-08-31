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

var tokens = [...]string{"HOST_KEY"}

type requestType int

const (
	requestTypeJsonConfigs = iota
	requestTypeConfigFiles
)

type DibsConfigsResponse struct {
	Buckets []string
	Configs map[string]interface{}
}

type DibsConfigFilesResponse struct {
	Files          map[string]string
	CurrentVersion string
}

func (s *HTTPServer) DibsJsonConfigs(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	return s.doDibsConfigs(resp, req, requestTypeJsonConfigs)
}

func (s *HTTPServer) DibsConfigFiles(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	return s.doDibsConfigs(resp, req, requestTypeConfigFiles)
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

	currentVersion, err := s.getValue(currentVersionKey)
	if err != nil {
		return nil, err
	}

	schema, err := s.getSchema(currentVersion)
	if err != nil {
		return nil, err
	}

	buckets, err := dibs.GetAllConfigBuckets(configBucket, schema, service, isLocal)
	if err != nil {
		return nil, err
	}

	tokensWithValues := make(map[string]string)
	for _, t := range tokens {
		tokensWithValues[t] = req.URL.Query().Get(t)
		if tokensWithValues[t] == "" {
			return nil, fmt.Errorf("must pass %s param", t)
		}
	}

	configs, err := s.getConfigs(currentVersion, service, buckets, tokensWithValues)
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
		files, err := dibs.GetBase64ConfigFiles(configs)
		if err != nil {
			return nil, err
		}

		return DibsConfigFilesResponse{
			Files:          files,
			CurrentVersion: currentVersion,
		}, nil
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

func (s *HTTPServer) getConfigs(currentVersion, service string, buckets []string, tokensWithValues map[string]string) (map[string]string, error) {
	var configs map[string]string
	allConfigs, err := s.getValues(beServicesPrefix + "/" + currentVersion + "/")
	if err != nil {
		return nil, err
	}

	configs, err = dibs.GetConfigs(buckets, allConfigs, tokensWithValues)
	if err != nil {
		return nil, err
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
