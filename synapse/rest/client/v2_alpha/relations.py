# -*- coding: utf-8 -*-
# Copyright 2019 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from twisted.internet import defer

from synapse.api.constants import EventTypes, RelationTypes
from synapse.api.errors import SynapseError
from synapse.http.servlet import (
    RestServlet,
    parse_integer,
    parse_json_object_from_request,
    parse_string,
)

from ._base import client_v2_patterns

logger = logging.getLogger(__name__)


class RelationSendServlet(RestServlet):
    PATTERN = (
        "/rooms/(?P<room_id>[^/]*)/send_relation"
        "/(?P<parent_id>[^/]*)/(?P<relation_type>[^/]*)/(?P<event_type>[^/]*)"
    )

    def __init__(self, hs):
        super(RelationSendServlet, self).__init__()
        self.auth = hs.get_auth()
        self.event_creation_handler = hs.get_event_creation_handler()

    def register(self, http_server):
        http_server.register_paths(
            "POST",
            client_v2_patterns(self.PATTERN + "$", releases=()),
            self.on_PUT_or_POST,
        )
        http_server.register_paths(
            "PUT",
            client_v2_patterns(self.PATTERN + "/(?P<txn_id>[^/]*)$", releases=()),
            self.on_PUT_or_POST,
        )

    @defer.inlineCallbacks
    def on_PUT_or_POST(
        self, request, room_id, parent_id, relation_type, event_type, txn_id=None
    ):
        requester = yield self.auth.get_user_by_req(request, allow_guest=True)

        if event_type == EventTypes.Member:
            # FIXME ??????
            raise SynapseError(400, "Cannot send member events with relations")

        content = parse_json_object_from_request(request)

        aggregation_key = parse_string(request, "key", encoding="utf-8")

        content["m.relates_to"] = {
            "event_id": parent_id,
            "key": aggregation_key,
            "rel_type": relation_type,
        }

        event_dict = {
            "type": event_type,
            "content": content,
            "room_id": room_id,
            "sender": requester.user.to_string(),
        }

        event = yield self.event_creation_handler.create_and_send_nonmember_event(
            requester, event_dict=event_dict, txn_id=txn_id
        )

        defer.returnValue((200, {"event_id": event.event_id}))


class RelationPaginationServlet(RestServlet):
    PATTERNS = client_v2_patterns(
        "/rooms/(?P<room_id>[^/]*)/relations/(?P<parent_id>[^/]*)"
        "(/(?P<relation_type>[^/]*)(/(?P<event_type>[^/]*))?)?$",
        releases=(),
    )

    def __init__(self, hs):
        super(RelationPaginationServlet, self).__init__()
        self.auth = hs.get_auth()
        self.store = hs.get_datastore()

    @defer.inlineCallbacks
    def on_GET(self, request, room_id, parent_id, relation_type=None, event_type=None):
        requester = yield self.auth.get_user_by_req(request, allow_guest=True)

        yield self.auth.check_in_room_or_world_readable(
            room_id, requester.user.to_string()
        )

        limit = parse_integer(request, "limit", default=5)

        # FIXME: Do filtering?

        res = yield self.store.get_relations_for_event(
            event_id=parent_id,
            relation_type=relation_type,
            event_type=event_type,
            limit=limit,
        )

        defer.returnValue((200, res.to_dict()))


class RelationAggregationPaginationServlet(RestServlet):
    PATTERNS = client_v2_patterns(
        "/rooms/(?P<room_id>[^/]*)/aggregations/(?P<parent_id>[^/]*)"
        "(/(?P<relation_type>[^/]*)(/(?P<event_type>[^/]*))?)?$",
        releases=(),
    )

    def __init__(self, hs):
        super(RelationAggregationPaginationServlet, self).__init__()
        self.auth = hs.get_auth()
        self.store = hs.get_datastore()

    @defer.inlineCallbacks
    def on_GET(self, request, room_id, parent_id, relation_type=None, event_type=None):
        requester = yield self.auth.get_user_by_req(request, allow_guest=True)

        yield self.auth.check_in_room_or_world_readable(
            room_id, requester.user.to_string()
        )

        if relation_type not in (RelationTypes.ANNOTATION, None):
            raise SynapseError(400, "Relation type must be 'annotation'")

        limit = parse_integer(request, "limit", default=5)

        # FIXME: Do filtering?

        res = yield self.store.get_aggregation_groups_for_event(
            event_id=parent_id, event_type=event_type, limit=limit
        )

        defer.returnValue((200, res.to_dict()))


def register_servlets(hs, http_server):
    RelationSendServlet(hs).register(http_server)
    RelationPaginationServlet(hs).register(http_server)
    RelationAggregationPaginationServlet(hs).register(http_server)
