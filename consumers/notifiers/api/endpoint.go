// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"

	notifiers "github.com/absmach/supermq-contrib/consumers/notifiers"
	api "github.com/absmach/supermq/api/http"
	apiutil "github.com/absmach/supermq/api/http/util"
	"github.com/absmach/supermq/pkg/authn"
	"github.com/absmach/supermq/pkg/errors"
	svcerr "github.com/absmach/supermq/pkg/errors/service"
	"github.com/go-kit/kit/endpoint"
)

func createSubscriptionEndpoint(svc notifiers.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(createSubReq)
		if err := req.validate(); err != nil {
			return createSubRes{}, errors.Wrap(apiutil.ErrValidation, err)
		}
		session, ok := ctx.Value(api.SessionKey).(authn.Session)
		if !ok {
			return nil, svcerr.ErrAuthentication
		}
		sub := notifiers.Subscription{
			Contact: req.Contact,
			Topic:   req.Topic,
		}
		id, err := svc.CreateSubscription(ctx, session, sub)
		if err != nil {
			return createSubRes{}, err
		}
		ucr := createSubRes{
			ID: id,
		}

		return ucr, nil
	}
}

func viewSubscriptionEndpint(svc notifiers.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(subReq)
		if err := req.validate(); err != nil {
			return viewSubRes{}, errors.Wrap(apiutil.ErrValidation, err)
		}
		session, ok := ctx.Value(api.SessionKey).(authn.Session)
		if !ok {
			return nil, svcerr.ErrAuthentication
		}
		sub, err := svc.ViewSubscription(ctx, session, req.id)
		if err != nil {
			return viewSubRes{}, err
		}
		res := viewSubRes{
			ID:      sub.ID,
			OwnerID: sub.OwnerID,
			Contact: sub.Contact,
			Topic:   sub.Topic,
		}
		return res, nil
	}
}

func listSubscriptionsEndpoint(svc notifiers.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(listSubsReq)
		if err := req.validate(); err != nil {
			return listSubsRes{}, errors.Wrap(apiutil.ErrValidation, err)
		}
		session, ok := ctx.Value(api.SessionKey).(authn.Session)
		if !ok {
			return nil, svcerr.ErrAuthentication
		}
		pm := notifiers.PageMetadata{
			Topic:   req.topic,
			Contact: req.contact,
			Offset:  req.offset,
			Limit:   int(req.limit),
		}
		page, err := svc.ListSubscriptions(ctx, session, pm)
		if err != nil {
			return listSubsRes{}, err
		}
		res := listSubsRes{
			Offset: page.Offset,
			Limit:  page.Limit,
			Total:  page.Total,
		}
		for _, sub := range page.Subscriptions {
			r := viewSubRes{
				ID:      sub.ID,
				OwnerID: sub.OwnerID,
				Contact: sub.Contact,
				Topic:   sub.Topic,
			}
			res.Subscriptions = append(res.Subscriptions, r)
		}

		return res, nil
	}
}

func deleteSubscriptionEndpint(svc notifiers.Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(subReq)
		if err := req.validate(); err != nil {
			return nil, errors.Wrap(apiutil.ErrValidation, err)
		}

		session, ok := ctx.Value(api.SessionKey).(authn.Session)
		if !ok {
			return nil, svcerr.ErrAuthentication
		}
		if err := svc.RemoveSubscription(ctx, session, req.id); err != nil {
			return nil, err
		}
		return removeSubRes{}, nil
	}
}
