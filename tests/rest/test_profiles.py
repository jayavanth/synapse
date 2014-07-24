# -*- coding: utf-8 -*-
"""Tests REST events for /profile paths."""

from twisted.trial import unittest
from twisted.internet import defer

from mock import Mock

from ..utils import MockHttpServer

from synapse.api.errors import SynapseError, AuthError
from synapse.server import HomeServer

myid = "@1234ABCD:test"


class ProfilesTestCase(unittest.TestCase):
    """ Tests profile management. """

    def mock_get_user_by_token(self, token=None):
        return "1234ABCD"  # local part only

    def setUp(self):
        self.mock_server = MockHttpServer()
        self.mock_handler = Mock(spec=[
            "get_displayname",
            "set_displayname",
        ])

        hs = HomeServer("test",
                db_pool=None,
                http_server=self.mock_server)
        hs.get_auth().get_user_by_token = self.mock_get_user_by_token
        hs.get_handlers().profile_handler = self.mock_handler

        hs.register_servlets()

    @defer.inlineCallbacks
    def test_get_my_name(self):
        mocked_get = self.mock_handler.get_displayname
        mocked_get.return_value = defer.succeed("Frank")

        (code, response) = yield self.mock_server.trigger("GET",
                "/profile/%s/displayname" % (myid), None)

        self.assertEquals(200, code)
        self.assertEquals("Frank", response)
        self.assertEquals(mocked_get.call_args[0][0].localpart, "1234ABCD")

    @defer.inlineCallbacks
    def test_set_my_name(self):
        mocked_set = self.mock_handler.set_displayname
        mocked_set.return_value = defer.succeed(())

        (code, response) = yield self.mock_server.trigger("PUT",
                "/profile/%s/displayname" % (myid), '"Frank Jr."')

        self.assertEquals(200, code)
        self.assertEquals(mocked_set.call_args[0][0].localpart, "1234ABCD")
        self.assertEquals(mocked_set.call_args[0][1], "1234ABCD")
        self.assertEquals(mocked_set.call_args[0][2], "Frank Jr.")

    @defer.inlineCallbacks
    def test_set_my_name_noauth(self):
        mocked_set = self.mock_handler.set_displayname
        mocked_set.side_effect = AuthError(400, "message")

        (code, response) = yield self.mock_server.trigger("PUT",
                "/profile/%s/displayname" % ("@4567:test"), '"Frank Jr."')

        self.assertTrue(400 <= code < 499,
                msg="code %d is in the 4xx range" % (code))

    @defer.inlineCallbacks
    def test_get_other_name(self):
        mocked_get = self.mock_handler.get_displayname
        mocked_get.return_value = defer.succeed("Bob")

        (code, response) = yield self.mock_server.trigger("GET",
                "/profile/%s/displayname" % ("@opaque:elsewhere"), None)

        self.assertEquals(200, code)
        self.assertEquals("Bob", response)

    @defer.inlineCallbacks
    def test_set_other_name(self):
        mocked_set = self.mock_handler.set_displayname
        mocked_set.side_effect = SynapseError(400, "message")

        (code, response) = yield self.mock_server.trigger("PUT",
                "/profile/%s/displayname" % ("@opaque:elsewhere"), None)

        self.assertTrue(400 <= code <= 499,
                msg="code %d is in the 4xx range" % (code))
