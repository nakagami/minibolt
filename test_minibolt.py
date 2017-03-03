#!/usr/bin/env python3
###############################################################################
# MIT License
#
# Copyright (c) 2016 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
import sys
import unittest
import binascii
import minibolt

try:
    from collections import OrderedDict
except ImportError:
    from ucollections import OrderedDict


class TestNeo4jBolt(unittest.TestCase):
    host = '127.0.0.1'
    user = 'neo4j'
    password = 'secret'

    def test_encode(self):
        m = minibolt.ResetMessage()
        self.assertEqual(
            binascii.b2a_hex(m.encode()).decode('utf-8').upper(),
            'B00F'
        )
        self.assertEqual(m.encode_item(None), b'\xC0')
        self.assertEqual(m.encode_item(True), b'\xC3')
        self.assertEqual(m.encode_item(False), b'\xC2')
        self.assertEqual(m.encode_item(1), b'\x01')
        self.assertEqual(
            m.encode_item(-9223372036854775808),
            b'\xCB\x80\x00\x00\x00\x00\x00\x00\x00',
        )

        authToken = OrderedDict()
        authToken["scheme"] = "basic"
        authToken["principal"] = "neo4j"
        authToken["credentials"] = "secret"

        m = minibolt.InitMessage("MyClient/1.0", authToken)
        hs = binascii.b2a_hex(m.encode()).decode('utf-8').upper()
        self.assertEqual(hs, 'B1018C4D79436C69656E742F312E30A386736368656D65856261736963897072696E636970616C856E656F346A8B63726564656E7469616C7386736563726574')


        m = minibolt.RunMessage("RETURN 1 AS num", {})
        hs = binascii.b2a_hex(m.encode()).decode('utf-8').upper()
        self.assertEqual(hs, 'B2108F52455455524E2031204153206E756DA0')


    def test_connect(self):
        try:
            conn = minibolt.connect(self.host, self.user, 'Evil Password')
        except minibolt.FailureMessage as e:
            self.assertEqual(e.data['code'], 'Neo.ClientError.Security.Unauthorized')
            self.assertEqual(e.data['message'], 'The client is unauthorized due to authentication failure.')
        conn = minibolt.connect(self.host, self.user, self.password)
        self.assertEqual(conn.run('RETURN 1 AS num'), [[1]])
#        for r in conn.run('MATCH (people:Person) RETURN people LIMIT 10'):
#            print(r)
#        for rs in  conn.run('MATCH (tom:Person {name: "Tom Hanks"})-[:ACTED_IN]->(tomHanksMovies) RETURN tom,tomHanksMovies'):
#            for r in rs:
#                print(r)

        conn.close()

if __name__ == "__main__":
    unittest.main()
