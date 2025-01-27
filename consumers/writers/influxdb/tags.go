// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package influxdb

import (
	"github.com/absmach/supermq/pkg/transformers/json"
	"github.com/absmach/supermq/pkg/transformers/senml"
)

type tags map[string]string

func senmlTags(msg senml.Message) tags {
	return tags{
		"channel":   msg.Channel,
		"subtopic":  msg.Subtopic,
		"publisher": msg.Publisher,
		"name":      msg.Name,
	}
}

func jsonTags(msg json.Message) tags {
	return tags{
		"channel":   msg.Channel,
		"subtopic":  msg.Subtopic,
		"publisher": msg.Publisher,
	}
}
