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

import attr

from twisted.internet import defer

from synapse.api.constants import EventTypes, RelationTypes
from synapse.api.errors import SynapseError
from synapse.storage._base import SQLBaseStore
from synapse.storage.stream import generic_bound

logger = logging.getLogger(__name__)


@attr.s
class PaginationChunk(object):
    chunk = attr.ib()
    next_batch = attr.ib(default=None)
    prev_batch = attr.ib(default=None)

    def to_dict(self):
        d = {"chunk": self.chunk}

        if self.next_batch:
            d["next_batch"] = self.next_batch.to_string()

        if self.prev_batch:
            d["prev_batch"] = self.prev_batch.to_string()

        return d


@attr.s
class RelationPaginationToken(object):
    topological = attr.ib()
    stream = attr.ib()

    @staticmethod
    def from_string(string):
        try:
            t, s = string.split("-")
            return RelationPaginationToken(int(t), int(s))
        except ValueError:
            raise SynapseError(400, "Invalid token")

    def to_string(self):
        return "%d-%d" % (self.topological, self.stream)


@attr.s
class AggregationPaginationToken(object):
    count = attr.ib()
    stream = attr.ib()

    @staticmethod
    def from_string(string):
        try:
            c, s = string.split("-")
            return AggregationPaginationToken(int(c), int(s))
        except ValueError:
            raise SynapseError(400, "Invalid token")

    def to_string(self):
        return "%d-%d" % (self.count, self.stream)


class RelationsStore(SQLBaseStore):
    def get_relations_for_event(
        self,
        event_id,
        relation_type=None,
        event_type=None,
        aggregation_key=None,
        limit=5,
        direction="b",
        from_token=None,
        to_token=None,
    ):
        """
        """

        if from_token:
            from_token = RelationPaginationToken.from_string(from_token)

        if to_token:
            to_token = RelationPaginationToken.from_string(to_token)

        where_clause = ["relates_to_id = ?"]
        where_args = [event_id]

        if relation_type:
            where_clause.append("relation_type = ?")
            where_args.append(relation_type)

        if event_type:
            where_clause.append("type = ?")
            where_args.append(event_type)

        if aggregation_key:
            where_clause.append("aggregation_key = ?")
            where_args.append(aggregation_key)

        if direction == "b":
            order = "DESC"

            if from_token:
                where_clause.append(
                    generic_bound(
                        bound=">=",
                        names=("topological_ordering", "stream_ordering"),
                        values=(from_token.topological, from_token.stream),
                        engine=self.database_engine,
                    )
                )

            if to_token:
                where_clause.append(
                    generic_bound(
                        bound="<",
                        names=("topological_ordering", "stream_ordering"),
                        values=(to_token.topological, to_token.stream),
                        engine=self.database_engine,
                    )
                )
        else:
            order = "ASC"

            if from_token:
                where_clause.append(
                    generic_bound(
                        bound="<",
                        names=("topological_ordering", "stream_ordering"),
                        values=(from_token.topological, from_token.stream),
                        engine=self.database_engine,
                    )
                )

            if to_token:
                where_clause.append(
                    generic_bound(
                        bound=">=",
                        names=("topological_ordering", "stream_ordering"),
                        values=(to_token.topological, to_token.stream),
                        engine=self.database_engine,
                    )
                )

        sql = """
            SELECT event_id, topological_ordering, stream_ordering
            FROM event_relations
            INNER JOIN events USING (event_id)
            WHERE %s
            ORDER BY topological_ordering %s, stream_ordering %s
            LIMIT ?
        """ % (
            " AND ".join(where_clause),
            order,
            order,
        )

        def _get_recent_references_for_event_txn(txn):
            txn.execute(sql, where_args + [limit + 1])

            last_topo_id = None
            last_stream_id = None
            events = []
            for row in txn:
                events.append({"event_id": row[0]})
                last_topo_id = row[1]
                last_stream_id = row[2]

            next_batch = None
            if len(events) > limit and last_topo_id and last_stream_id:
                next_batch = RelationPaginationToken(last_topo_id, last_stream_id)

            return PaginationChunk(
                chunk=list(events[:limit]), next_batch=next_batch, prev_batch=from_token
            )

        return self.runInteraction(
            "get_recent_references_for_event", _get_recent_references_for_event_txn
        )

    def get_aggregation_groups_for_event(
        self,
        event_id,
        event_type=None,
        limit=5,
        direction="b",
        from_token=None,
        to_token=None,
    ):
        if from_token:
            from_token = AggregationPaginationToken.from_string(from_token)

        if to_token:
            to_token = AggregationPaginationToken.from_string(to_token)

        where_clause = ["relates_to_id = ?", "relation_type = ?"]
        where_args = [event_id, RelationTypes.ANNOTATION]

        if event_type:
            where_clause.append("type = ?")
            where_args.append(event_type)

        having_clause = []
        if direction == "b":
            order = "DESC"

            if from_token:
                having_clause.append(
                    generic_bound(
                        bound=">=",
                        names=("COUNT(*)", "MAX(stream_ordering)"),
                        values=(from_token.count, from_token.stream),
                        engine=self.database_engine,
                    )
                )

            if to_token:
                having_clause.append(
                    generic_bound(
                        bound="<",
                        names=("COUNT(*)", "MAX(stream_ordering)"),
                        values=(to_token.count, to_token.stream),
                        engine=self.database_engine,
                    )
                )
        else:
            order = "ASC"

            if from_token:
                having_clause.append(
                    generic_bound(
                        bound="<",
                        names=("COUNT(*)", "MAX(stream_ordering)"),
                        values=(from_token.count, from_token.stream),
                        engine=self.database_engine,
                    )
                )

            if to_token:
                having_clause.append(
                    generic_bound(
                        bound=">=",
                        names=("COUNT(*)", "MAX(stream_ordering)"),
                        values=(to_token.count, to_token.stream),
                        engine=self.database_engine,
                    )
                )

        if having_clause:
            having_clause = "HAVING " + " AND ".join(having_clause)
        else:
            having_clause = ""

        sql = """
            SELECT type, aggregation_key, COUNT(*), MAX(stream_ordering)
            FROM event_relations
            INNER JOIN events USING (event_id)
            WHERE {where_clause}
            GROUP BY relation_type, type, aggregation_key
            {having_clause}
            ORDER BY COUNT(*) {order}, MAX(stream_ordering) {order}
            LIMIT ?
        """.format(
            where_clause=" AND ".join(where_clause),
            order=order,
            having_clause=having_clause,
        )

        def _get_aggregation_groups_for_event_txn(txn):
            txn.execute(sql, where_args + [limit + 1])

            next_batch = None
            events = []
            for row in txn:
                events.append({"type": row[0], "key": row[1], "count": row[2]})
                next_batch = AggregationPaginationToken(row[2], row[3])

            if len(events) <= limit:
                next_batch = None

            return PaginationChunk(
                chunk=list(events[:limit]), next_batch=next_batch, prev_batch=from_token
            )

        return self.runInteraction(
            "get_aggregation_groups_for_event", _get_aggregation_groups_for_event_txn
        )

    @defer.inlineCallbacks
    def get_applicable_edit(self, event):
        if event.type != EventTypes.Message:
            return

        sql = """
            SELECT event_id, origin_server_ts FROM events
            INNER JOIN event_relations USING (event_id)
            WHERE
                relates_to_id = ?
                AND relation_type = ?
                AND type = ?
                AND sender = ?
            ORDER by origin_server_ts DESC, event_id DESC
            LIMIT 1
        """

        def _get_applicable_edit_txn(txn):
            txn.execute(
                sql, (event.event_id, RelationTypes.REPLACES, event.type, event.sender)
            )
            row = txn.fetchone()
            if row:
                return row[0]

        event_id = yield self.runInteraction(
            "get_applicable_edit", _get_applicable_edit_txn
        )

        if not event_id:
            return

        edit_event = yield self.get_event(event_id, allow_none=True)
        return edit_event

    def _handle_event_relations(self, txn, event):
        relation = event.content.get("m.relates_to")
        if not relation:
            # No relations
            return

        rel_type = relation.get("rel_type")
        if rel_type not in (
            RelationTypes.ANNOTATION,
            RelationTypes.REFERENCES,
            RelationTypes.REPLACES,
        ):
            # Unknown relation type
            return

        parent_id = relation.get("event_id")
        if not parent_id:
            # Invalid relation
            return

        aggregation_key = relation.get("key")

        self._simple_insert_txn(
            txn,
            table="event_relations",
            values={
                "event_id": event.event_id,
                "relates_to_id": parent_id,
                "relation_type": rel_type,
                "aggregation_key": aggregation_key,
            },
        )
