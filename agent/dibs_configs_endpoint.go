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

type DibsConfigsResponse struct {
	Name string `json:"name"`
}

func (s *HTTPServer) DibsConfigs(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	configBucket := req.URL.Query().Get("configBucket")
	if configBucket == "" {
		return nil, fmt.Errorf("must pass configBucket param")
	}

	isLocal := req.URL.Query().Get("local") == "true"

	currentVersion, err := s.getValue(currentVersionKey)
	if err != nil {
		return nil, err
	}

	schemaJSON, err := s.getValue(beServicesPrefix + "/" + currentVersion + "/" + schemaFileName)
	if err != nil {
		return nil, err
	}

	var schema map[string]map[string]interface{}
	json.Unmarshal([]byte(schemaJSON), &schema)

	buckets, err := dibs.GetAllConfigBuckets(configBucket, schema, isLocal)
	if err != nil {
		return nil, err
	}

	allConfigs, err := s.getValues(beServicesPrefix + "/" + currentVersion + "/")
	if err != nil {
		return nil, err
	}

	configs, err := dibs.GetConfigs(buckets, allConfigs)
	if err != nil {
		return nil, err
	}

	// args := structs.KeyListRequest{
	// 	Datacenter: "dibs-consul",
	// }

	// var out structs.IndexedKeyList
	// if err := s.agent.RPC("KVS.ListKeys", &args, &out); err != nil {
	// 	return nil, err
	// }

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
