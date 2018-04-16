// Copyright 2016 ~ 2018 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of this source code is
// governed by Apache License 2.0.
//
// refer: https://github.com/micro/go-micro/blob/master/registry/service.go
package gxregistry

import (
	"encoding/json"
	"net/url"
	"strings"
)

import (
	"github.com/AlexStocks/goext/strings"
	jerrors "github.com/juju/errors"
)

type ServiceAttr struct {
	Group    string          `json:"group,omitempty"`
	Name     string          `json:"name,omitempty"`     // service name
	Protocol string          `json:"protocol,omitempty"` // codec
	Version  string          `json:"version,omitempty"`
	Role     ServiceRoleType `json:"role,omitempty"`
}

type Service struct {
	ServiceAttr
	Nodes    []*Node           `json:"nodes,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Node struct {
	ID       string            `json:"id,omitempty"`
	Address  string            `json:"address,omitempty"`
	Port     int               `json:"port,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (a ServiceAttr) Marshal() ([]byte, error) {
	params := url.Values{}
	params.Add("group", a.Group)
	params.Add("service", a.Name)
	params.Add("protocol", a.Protocol)
	params.Add("version", a.Version)
	params.Add("role", a.Role.String())

	// encode result: group=bjtelecom&protocol=pb&service=shopping&type=add+service&version=1.0.1
	// query escape: group%3Dbjtelecom%26protocol%3Dpb%26service%3Dshopping%26type%3Dadd%2Bservice%26version%3D1.0.1
	return []byte(url.QueryEscape(params.Encode())), nil
}

func (a *ServiceAttr) Unmarshal(data []byte) error {
	rawString, err := url.QueryUnescape(gxstrings.String(data))
	if err != nil {
		return jerrors.Annotatef(err, "url.QueryUnescape(data:%s)", gxstrings.String(data))
	}

	query, err := url.ParseQuery(rawString)
	if err != nil {
		return jerrors.Annotatef(err, "url.ParseQuery(string:%s)", rawString)
	}

	a.Group = query.Get("group")
	a.Name = query.Get("service")
	a.Protocol = query.Get("protocol")
	a.Version = query.Get("version")
	(&a.Role).Get(query.Get("role"))

	return nil
}

func EncodeService(s *Service) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", jerrors.Annotatef(err, "json.Marshal(Service:%+v)", s)
	}

	return string(b), nil
}

func DecodeService(ds []byte) (*Service, error) {
	var s *Service
	err := json.Unmarshal(ds, &s)
	if err != nil {
		return nil, jerrors.Annotatef(err, "json.Unmarshal()")
	}
	return s, nil
}

func registryPath(paths ...string) string {
	var service strings.Builder
	for _, path := range paths {
		if path != "" {
			service.WriteString(path)
			if !strings.HasSuffix(path, "/") {
				service.WriteString("/")
			}
		}
	}

	return service.String()
}

// Path example: /dubbo/shopping-bjtelecom-pb-1.0.1-provider/node1
func (s *Service) Path(root string) string {
	params := url.Values{}
	params.Add("group", s.Group)
	params.Add("service", s.Name)
	params.Add("protocol", s.Protocol)
	params.Add("version", s.Version)
	params.Add("role", s.Role.String())
	// encode result: group=bjtelecom&protocol=pb&service=shopping&type=add+service&version=1.0.1
	// query escape: group%3Dbjtelecom%26protocol%3Dpb%26service%3Dshopping%26type%3Dadd%2Bservice%26version%3D1.0.1
	service_path := url.QueryEscape(params.Encode())

	return registryPath([]string{root, service_path}...)
}

func (s *Service) NodePath(root string, node Node) string {
	return s.Path(root) + node.ID
}
