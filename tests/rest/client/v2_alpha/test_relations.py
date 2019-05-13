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

from synapse.api.constants import EventTypes, RelationTypes
from synapse.rest.client.v1 import login, room
from synapse.rest.client.v2_alpha import relations

import six

from tests import unittest


class RelationsTestCase(unittest.HomeserverTestCase):
    user_id = "@alice:test"
    servlets = [
        relations.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor, clock, hs):
        self.room = self.helper.create_room_as(self.user_id)
        res = self.helper.send(self.room, body="Hi!")
        self.parent_id = res["event_id"]

    def test_send_relation(self):
        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", key="👍")
        self.assertEquals(200, channel.code, channel.json_body)

        event_id = channel.json_body["event_id"]

        request, channel = self.make_request(
            "GET", "/rooms/%s/event/%s" % (self.room, event_id)
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.assert_dict(
            {
                "type": "m.reaction",
                "sender": self.user_id,
                "content": {
                    "m.relates_to": {
                        "event_id": self.parent_id,
                        "key": "👍",
                        "rel_type": RelationTypes.ANNOTATION,
                    }
                },
            },
            channel.json_body,
        )

    def test_deny_membership(self):
        channel = self._send_relation(RelationTypes.ANNOTATION, EventTypes.Member)
        self.assertEquals(400, channel.code, channel.json_body)

    def test_paginate(self):
        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction")
        self.assertEquals(200, channel.code, channel.json_body)
        annotation_id = channel.json_body["event_id"]

        request, channel = self.make_request(
            "GET",
            "/_matrix/client/unstable/rooms/%s/relations/%s?limit=1"
            % (self.room, self.parent_id),
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.assertEquals(
            channel.json_body,
            {
                "limited": True,
                "chunk": [{"event_id": annotation_id}],
                "prev_batch": None,
                "next_batch": None,
            },
        )

    def test_aggregation(self):
        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "a")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "a")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "b")
        self.assertEquals(200, channel.code, channel.json_body)

        request, channel = self.make_request(
            "GET",
            "/_matrix/client/unstable/rooms/%s/aggregations/%s"
            % (self.room, self.parent_id),
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.assertEquals(
            channel.json_body,
            {
                "limited": False,
                "chunk": [
                    {"type": "m.reaction", "key": "a", "count": 2},
                    {"type": "m.reaction", "key": "b", "count": 1},
                ],
                "prev_batch": None,
                "next_batch": None,
            },
        )

    def test_aggregation_must_be_annotation(self):
        request, channel = self.make_request(
            "GET",
            "/_matrix/client/unstable/rooms/%s/aggregations/m.replaces/%s?limit=1"
            % (self.room, self.parent_id),
        )
        self.render(request)
        self.assertEquals(400, channel.code, channel.json_body)

    def test_aggregation_get_event(self):
        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "a")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "a")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", "b")
        self.assertEquals(200, channel.code, channel.json_body)

        channel = self._send_relation(RelationTypes.REFERENCES, "m.room.test")
        self.assertEquals(200, channel.code, channel.json_body)
        reply_1 = channel.json_body["event_id"]

        channel = self._send_relation(RelationTypes.REFERENCES, "m.room.test")
        self.assertEquals(200, channel.code, channel.json_body)
        reply_2 = channel.json_body["event_id"]

        request, channel = self.make_request(
            "GET", "/rooms/%s/event/%s" % (self.room, self.parent_id)
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.maxDiff = None

        self.assertEquals(
            channel.json_body["unsigned"].get("m.relations"),
            {
                RelationTypes.ANNOTATION: {
                    "chunk": [
                        {"type": "m.reaction", "key": "a", "count": 2},
                        {"type": "m.reaction", "key": "b", "count": 1},
                    ],
                    "limited": False,
                    "next_batch": None,
                    "prev_batch": None,
                },
                RelationTypes.REFERENCES: {
                    "chunk": [{"event_id": reply_1}, {"event_id": reply_2}],
                    "limited": False,
                    "next_batch": None,
                    "prev_batch": None,
                },
            },
        )

    def _send_relation(self, relation_type, event_type, key=None):
        query = ""
        if key:
            query = "?key=" + six.moves.urllib.parse.quote_plus(key)

        request, channel = self.make_request(
            "POST",
            "/_matrix/client/unstable/rooms/%s/send_relation/%s/%s/%s%s"
            % (self.room, self.parent_id, relation_type, event_type, query),
            b"{}",
        )
        self.render(request)
        return channel
