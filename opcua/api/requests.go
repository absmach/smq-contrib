// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import apiutil "github.com/absmach/supermq/api/http/util"

type browseReq struct {
	ServerURI      string
	Namespace      string
	Identifier     string
	IdentifierType string
}

func (req *browseReq) validate() error {
	if req.ServerURI == "" {
		return apiutil.ErrMissingID
	}

	return nil
}
