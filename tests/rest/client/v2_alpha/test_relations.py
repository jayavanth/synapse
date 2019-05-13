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

import itertools
import json

import six

from synapse.api.constants import EventTypes, RelationTypes
from synapse.rest.client.v1 import login, room
from synapse.rest.client.v2_alpha import relations

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

    def test_basic_paginate_relations(self):
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

        # Check that the result only has "chunk" and "next_batch" keys, and that
        # next_batch looks maybe correct.
        self.assertEquals(
            set(channel.json_body.keys()), {"chunk", "next_batch"}, channel.json_body
        )
        self.assertEquals(len(channel.json_body["chunk"]), 1, channel.json_body)

        self.assert_dict({"event_id": annotation_id}, channel.json_body["chunk"][0])
        self.assertIsInstance(
            channel.json_body.get("next_batch"), six.string_types, channel.json_body
        )

    def test_repeated_paginate_relations(self):
        expected_event_ids = []
        for _ in range(10):
            channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction")
            self.assertEquals(200, channel.code, channel.json_body)
            expected_event_ids.append(channel.json_body["event_id"])

        prev_token = None
        found_event_ids = []
        for _ in range(20):
            from_token = ""
            if prev_token:
                from_token = "&from=" + prev_token

            request, channel = self.make_request(
                "GET",
                "/_matrix/client/unstable/rooms/%s/relations/%s?limit=1%s"
                % (self.room, self.parent_id, from_token),
            )
            self.render(request)
            self.assertEquals(200, channel.code, channel.json_body)

            found_event_ids.extend(e["event_id"] for e in channel.json_body["chunk"])
            next_batch = channel.json_body.get("next_batch")

            self.assertNotEquals(prev_token, next_batch)
            prev_token = next_batch

            if not prev_token:
                break

        # We paginated backwards, so reverse
        found_event_ids.reverse()
        self.assertEquals(found_event_ids, expected_event_ids)

    def test_aggregation_pagination_groups(self):
        sent_groups = {"👍": 10, "a": 7, "b": 5, "c": 3, "d": 2, "e": 1}
        for key in itertools.chain.from_iterable(
            itertools.repeat(key, num) for key, num in sent_groups.items()
        ):
            channel = self._send_relation(
                RelationTypes.ANNOTATION, "m.reaction", key=key
            )
            self.assertEquals(200, channel.code, channel.json_body)

        prev_token = None
        found_groups = {}
        for _ in range(20):
            from_token = ""
            if prev_token:
                from_token = "&from=" + prev_token

            request, channel = self.make_request(
                "GET",
                "/_matrix/client/unstable/rooms/%s/aggregations/%s?limit=1%s"
                % (self.room, self.parent_id, from_token),
            )
            self.render(request)
            self.assertEquals(200, channel.code, channel.json_body)

            self.assertEqual(len(channel.json_body["chunk"]), 1, channel.json_body)

            for groups in channel.json_body["chunk"]:
                # We only expect reactions
                self.assertEqual(groups["type"], "m.reaction", channel.json_body)

                # We should only see each key once
                self.assertNotIn(groups["key"], found_groups, channel.json_body)

                found_groups[groups["key"]] = groups["count"]

            next_batch = channel.json_body.get("next_batch")

            self.assertNotEquals(prev_token, next_batch)
            prev_token = next_batch

            if not prev_token:
                break

        self.assertEquals(sent_groups, found_groups)

    def test_aggregation_pagination_within_group(self):
        expected_event_ids = []
        for _ in range(10):
            channel = self._send_relation(
                RelationTypes.ANNOTATION, "m.reaction", key="👍"
            )
            self.assertEquals(200, channel.code, channel.json_body)
            expected_event_ids.append(channel.json_body["event_id"])

        # Also send a different type of reaction so that we test we don't see it
        channel = self._send_relation(RelationTypes.ANNOTATION, "m.reaction", key="a")
        self.assertEquals(200, channel.code, channel.json_body)

        prev_token = None
        found_event_ids = []
        encoded_key = six.moves.urllib.parse.quote_plus("👍")
        for _ in range(20):
            from_token = ""
            if prev_token:
                from_token = "&from=" + prev_token

            request, channel = self.make_request(
                "GET",
                "/_matrix/client/unstable/rooms/%s"
                "/aggregations/%s/%s/m.reaction/%s?limit=1%s"
                % (
                    self.room,
                    self.parent_id,
                    RelationTypes.ANNOTATION,
                    encoded_key,
                    from_token,
                ),
            )
            self.render(request)
            self.assertEquals(200, channel.code, channel.json_body)

            self.assertEqual(len(channel.json_body["chunk"]), 1, channel.json_body)

            found_event_ids.extend(e["event_id"] for e in channel.json_body["chunk"])

            next_batch = channel.json_body.get("next_batch")

            self.assertNotEquals(prev_token, next_batch)
            prev_token = next_batch

            if not prev_token:
                break

        # We paginated backwards, so reverse
        found_event_ids.reverse()
        self.assertEquals(found_event_ids, expected_event_ids)

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
                "chunk": [
                    {"type": "m.reaction", "key": "a", "count": 2},
                    {"type": "m.reaction", "key": "b", "count": 1},
                ]
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

        self.assertEquals(
            channel.json_body["unsigned"].get("m.relations"),
            {
                RelationTypes.ANNOTATION: {
                    "chunk": [
                        {"type": "m.reaction", "key": "a", "count": 2},
                        {"type": "m.reaction", "key": "b", "count": 1},
                    ]
                },
                RelationTypes.REFERENCES: {
                    "chunk": [{"event_id": reply_1}, {"event_id": reply_2}]
                },
            },
        )

    def test_edit(self):
        new_body = {"msgtype": "m.text", "body": "I've been edited!"}
        channel = self._send_relation(
            RelationTypes.REPLACES, "m.room.message", content=new_body
        )
        self.assertEquals(200, channel.code, channel.json_body)

        edit_event_id = channel.json_body["event_id"]

        request, channel = self.make_request(
            "GET", "/rooms/%s/event/%s" % (self.room, self.parent_id)
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.assertEquals(channel.json_body["content"], new_body)

        self.assertEquals(
            channel.json_body["unsigned"].get("m.relations"),
            {RelationTypes.REPLACES: {"event_id": edit_event_id}},
        )

    def test_multi_edit(self):
        channel = self._send_relation(
            RelationTypes.REPLACES,
            "m.room.message",
            content={"msgtype": "m.text", "body": "First edit"},
        )
        self.assertEquals(200, channel.code, channel.json_body)

        new_body = {"msgtype": "m.text", "body": "I've been edited!"}
        channel = self._send_relation(
            RelationTypes.REPLACES, "m.room.message", content=new_body
        )
        self.assertEquals(200, channel.code, channel.json_body)

        edit_event_id = channel.json_body["event_id"]

        channel = self._send_relation(
            RelationTypes.REPLACES,
            "m.room.message.WRONG_TYPE",
            content={"msgtype": "m.text", "body": "Edit, but wrong type"},
        )
        self.assertEquals(200, channel.code, channel.json_body)

        request, channel = self.make_request(
            "GET", "/rooms/%s/event/%s" % (self.room, self.parent_id)
        )
        self.render(request)
        self.assertEquals(200, channel.code, channel.json_body)

        self.assertEquals(channel.json_body["content"], new_body)

        self.assertEquals(
            channel.json_body["unsigned"].get("m.relations"),
            {RelationTypes.REPLACES: {"event_id": edit_event_id}},
        )

    def _send_relation(self, relation_type, event_type, key=None, content={}):
        query = ""
        if key:
            query = "?key=" + six.moves.urllib.parse.quote_plus(key)

        request, channel = self.make_request(
            "POST",
            "/_matrix/client/unstable/rooms/%s/send_relation/%s/%s/%s%s"
            % (self.room, self.parent_id, relation_type, event_type, query),
            json.dumps(content).encode("utf-8"),
        )
        self.render(request)
        return channel
