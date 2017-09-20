###############################################################################
# MIT License
#
# Copyright (c) 2017 Hajime Nakagami
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
try:
    import socket
    import struct
except ImportError:
    import usocket as socket
    import ustruct as struct

__version__ = '0.3.0'
__all__ = ('connect', 'to_nxgraph', 'nodes', 'relationships')


def int_to_bytes(n, ln):
    if n < 0:
        n += (1 << ln * 8)
    b = bytearray()
    for i in range(ln):
        b.append((n >> (i*8)) & 0xff)
    return bytes(reversed(b))


def int_from_bytes(b):
    r = 0
    for n in b:
        r = r * 256 + n
    return r


class Struct:
    def __init__(self, signature, size=0):
        self.signature = signature
        self.size = size

    def encode_item(self, v):
        if v is None:
            return bytes([0xC0])
        if v is True:
            return bytes([0xC3])
        elif v is False:
            return bytes([0xC2])
        elif isinstance(v, str):
            b = v.encode('utf-8')
            ln = len(b)
            if ln < 16:
                return bytes([0x80+len(b)]) + b
            elif ln < 256:
                return bytes([0xD0]) + int_to_bytes(ln, 1) + b
            elif ln < 65536:
                return bytes([0xD1]) + int_to_bytes(ln, 2) + b
            else:
                return bytes([0xD2]) + int_to_bytes(ln, 4) + b
        if isinstance(v, int):
            if v >= 0 and v <= 127:     # +TINY_INT
                return bytes([v])
            elif v < 0 and v >= -16:    # -TINY_INT
                return bytes([0b11110000 + v * -1])
            elif v >= -128 and v <= -17:    # INT_8
                return bytes([0xC8]) + int_to_bytes(v, 1)
            elif v >= -32768 and v <= 32767:  # INT_16
                return bytes([0xC9]) + int_to_bytes(v, 2)
            elif v >= -2147483648 and v <= 2147483647:  # INT_32
                return bytes([0xCA]) + int_to_bytes(v, 4)
            else:                       # INT_64
                return bytes([0xCB]) + int_to_bytes(v, 8)
        elif isinstance(v, dict):
            ln = len(v)
            if ln < 16:
                b = bytes([0xA0+ln])
            elif ln < 256:
                b = bytes([0xD8]) + int_to_bytes(ln, 1)
            elif ln < 65536:
                b = bytes([0xD9]) + int_to_bytes(ln, 2)
            else:
                b = bytes([0xDA]) + int_to_bytes(ln, 4)
            for k in v:
                b += self.encode_item(k)
                b += self.encode_item(v[k])
            return b

    def encode(self, args=[]):
        b = bytes([0xB0 + self.size, self.signature])
        for v in args:
            b += self.encode_item(v)
        return b


class InitMessage(Struct):
    def __init__(self, clientName, authToken):
        self.clientName = clientName
        self.authToken = authToken
        super().__init__(0x01, 1)

    def encode(self):
        return super().encode([self.clientName, self.authToken])


class RunMessage(Struct):
    def __init__(self, statement, parameters):
        self.statement = statement
        self.parameters = parameters
        super().__init__(0x10, 2)

    def encode(self):
        return super().encode([self.statement, self.parameters])


class DiscardAllMessage(Struct):
    def __init__(self):
        super().__init__(0x2F)


class PullAllMessage(Struct):
    def __init__(self):
        super().__init__(0x3F)


class AckFailureMessage(Struct):
    def __init__(self):
        super().__init__(0x0E)


class ResetMessage(Struct):
    def __init__(self):
        super().__init__(0x0F)


class RecordMessage(Struct):
    def __init__(self, args):
        self.data = args[0]
        super().__init__(0x71)

    def __str__(self):
        return "Record(%s)" % (str(self.data), )


class SuccessMessage(Struct):
    def __init__(self, args):
        self.data = args[0]
        super().__init__(0x70)

    def __str__(self):
        return str(self.data)


class FailureMessage(Struct, Exception):
    def __init__(self, args):
        self.data = args[0]
        super().__init__(0x7F)

    def __str__(self):
        return str(self.data)


class IgnoreMessage(Struct):
    def __init__(self, metadata):
        self.metadata = metadata
        super().__init__(0x7E)


class Node(Struct):
    def __init__(self, args):
        self.nodeIdentity = args[0]
        self.labels = args[1]
        self.properties = args[2]
        super().__init__(0x4E)

    def __getattr__(self, name):
        if name in self.properties:
            return self.properties[name]
        else:
            raise AttributeError

    def __str__(self):
        return "Node(%d:%s:%s" % (self.nodeIdentity, self.labels, self.properties)


class Relationship(Struct):
    def __init__(self, args):
        self.relIdentity = args[0]
        self.startNodeIdentity = args[1]
        self.endNodeIdentity = args[2]
        self.typeName = args[3]
        self.properties = args[4]
        super().__init__(0x52)

    def __getattr__(self, name):
        return self.properties[name]

    def __str__(self):
        return "Relationship(%d,%d->%d:%s:%s" % (
            self.relIdentity,
            self.startNodeIdentity,
            self.endNodeIdentity,
            self.typeName,
            self.properties
        )


class Path(Struct):
    def __init__(self, args):
        self._nodes = args[0]
        self._unbound_relationships = args[1]
        self._sequence = args[2]
        super().__init__(0x50)

    def nodes(self):
        return self._nodes

    def relationships(self):
        rs = []
        rel_seq = self._sequence[0::2]
        node_seq = [0] + self._sequence[1::2]
        for i in range(len(rel_seq)):
            unbound_relationship = self._unbound_relationships[abs(rel_seq[i]) -1]
            p, n = self._nodes[i], self._nodes[i + 1]
            if rel_seq[i] < 0:
                p, n = n, p
            rs.append(unbound_relationship.bind(p.nodeIdentity, n.nodeIdentity))
        return rs

    def __str__(self):
        return "Path(%s:%s:%s" % (
            ','.join([str(n) for n in self._nodes]),
            ','.join([str(r) for r in self._unbound_relationships]),
            self._sequence
        )


class UnboundRelationship(Struct):
    def __init__(self, args):
        self.relIdentity = args[0]
        self.typeName = args[1]
        self.properties = args[2]
        super().__init__(0x72)

    def bind(self, startNodeIdentity, endNodeIdentity):
        "create Relationship instance"
        return Relationship([
            self.relIdentity,
            startNodeIdentity,
            endNodeIdentity,
            self.typeName,
            self.properties
        ])

    def __getattr__(self, name):
        return self.properties[name]

    def __str__(self):
        return "UnboundRelationship(%d:%s:%s" % (
            self.relIdentity,
            self.typeName,
            self.properties,
        )


# -----------------------------------------------------------------------------

def decode_map(ln, message):
    r = {}
    for _ in range(ln):
        k, message = decode_message(message)
        v, message = decode_message(message)
        r[k] = v
    return r, message


def decode_list(ln, message):
    r = []
    for _ in range(ln):
        v, message = decode_message(message)
        r.append(v)
    return r, message


def decode_struct(ln, message):
    args = []
    signature = message[0]
    message = message[1:]
    for i in range(ln):
        arg, message = decode_message(message)
        args.append(arg)
    v = {
        0x01: InitMessage,
        0x10: RunMessage,
        0x2F: DiscardAllMessage,
        0x3F: PullAllMessage,
        0x0E: AckFailureMessage,
        0x0F: ResetMessage,
        0x71: RecordMessage,
        0x70: SuccessMessage,
        0x7F: FailureMessage,
        0x7E: IgnoreMessage,
        0x4E: Node,
        0x52: Relationship,
        0x50: Path,
        0x72: UnboundRelationship,
    }[signature](args)

    return v, message


def decode_message(message):
    marker, message = message[0], message[1:]
    # http://boltprotocol.org/v1/#marker_table
    if marker <= 0x7F:      # +TINY_INT
        return marker & 0b1111111, message
    elif marker <= 0x8F:    # TINY_STRING
        ln = marker & 0b1111
        v = message[:ln].decode('utf-8')
        return v, message[ln:]
    elif marker <= 0x9F:    # TINY_LIST
        ln = marker & 0b1111
        return decode_list(ln, message)
    elif marker <= 0xAF:    # TINY_MAP
        ln = marker & 0b1111
        return decode_map(ln, message)
    elif marker <= 0xBF:    # TINY_STRUCT
        ln = marker & 0b1111
        return decode_struct(ln, message)
    elif marker == 0xC0:
        return None, message
    elif marker == 0xC1:    # FLOAT_64
        v = struct.unpack('d', message[:8])[0]
        return v, message[8:]
    elif marker == 0xC2:
        return False, message
    elif marker == 0xC3:
        return True, message
    elif marker <= 0xC7:    # Reserverd
        raise ValueError('bad marker')
    elif marker == 0xC8:    # INT_8
        v = int_from_bytes(message[:1])
        return v, message[1:]
    elif marker == 0xC9:    # INT_16
        v = int_from_bytes(message[:2])
        return v, message[2:]
    elif marker == 0xCA:    # INT_32
        v = int_from_bytes(message[:4])
        return v, message[4:]
    elif marker == 0xCB:    # INT_64
        v = int_from_bytes(message[:8])
        return v, message[8:]
    elif marker <= 0xCF:    # Reserverd
        raise ValueError('bad marker')
    elif marker == 0xD0:    # STRING_8
        ln = int_from_bytes(message[:1])
        message = message[1:]
        v = message[:ln].decode('utf-8')
        return v, message[ln:]
    elif marker == 0xD1:    # STRING_16
        ln = int_from_bytes(message[:2])
        message = message[2:]
        v = message[:ln].decode('utf-8')
        return v, message[ln:]
    elif marker == 0xD2:    # STRING_32
        ln = int_from_bytes(message[:2])
        message = message[2:]
        v = message[:ln].decode('utf-8')
        return v, message[ln:]
    elif marker == 0xD3:    # Reserverd
        raise ValueError('bad marker')
    elif marker == 0xD4:    # LIST_8
        ln = int_from_bytes(message[:1])
        message = message[1:]
        return decode_list(ln, message)
    elif marker == 0xD5:    # LIST_16
        ln = int_from_bytes(message[:2])
        message = message[2:]
        return decode_list(ln, message)
    elif marker == 0xD6:    # LIST_32
        ln = int_from_bytes(message[:4])
        message = message[4:]
        return decode_list(ln, message)
    elif marker == 0xD7:    # Reserverd
        raise ValueError('bad marker')
    elif marker == 0xD8:    # MAP_8
        ln = int_from_bytes(message[:1])
        message = message[1:]
        return decode_map(ln, message)
    elif marker == 0xD9:    # MAP_16
        ln = int_from_bytes(message[:2])
        message = message[2:]
        return decode_map(ln, message)
    elif marker == 0xDA:    # MAP_32
        ln = int_from_bytes(message[:4])
        message = message[4:]
        return decode_map(ln, message)
    elif marker == 0xDB:    # Reserverd
        raise ValueError('bad marker')
    elif marker == 0xDC:    # STRUCT_8
        ln = int_from_bytes(message[:1])
        message = message[1:]
        return decode_struct(ln, message)
    elif marker == 0xDD:    # STRUCT_16
        ln = int_from_bytes(message[:2])
        message = message[2:]
        return decode_struct(ln, message)
    elif marker <= 0xEF:    # Reserverd
        raise ValueError('bad marker')
    else:                   # -TINY_INT
        return (16 -(marker & 0b1111)) * -1, message


class BoltSession:
    # http://boltprotocol.org/v1/
    def _send(self, b):
        n = 0
        while (n < len(b)):
            n += self._sock.send(b[n:])

    def _recv(self, ln):
        r = b''
        while len(r) < ln:
            b = self._sock.recv(ln-len(r))
            if not b:
                raise socket.error("Can't recv packets")
            r += b
        return r

    def _read_chunk(self):
        ln = int_from_bytes(self._recv(2))
        return self._recv(ln)

    def read_message(self):
        buf = b''
        b = self._read_chunk()
        while b:
            buf += b
            b = self._read_chunk()
        return buf

    def send_message(self, b):
        self._send(int_to_bytes(len(b), 2) + b)
        self._send(b'\x00\x00')     # End Marker

    def __init__(self, host, user, password, port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))

        # Initial handshake
        self._send(bytes([
            0x60, 0x60, 0xb0, 0x17,
            0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ]))
        assert self._recv(4) == b'\x00\x00\x00\x01'

        m = InitMessage(
            "PyBolt/" + __version__,
            {"scheme": "basic", "principal": self.user, "credentials": self.password},
        )
        self.send_message(m.encode())
        r, _ = decode_message(self.read_message())
        if isinstance(r, FailureMessage):
            raise r
        self.server_version = r.data['server']

    def run(self, query, params={}):
        m = RunMessage(query, params)
        self.send_message(m.encode())
        r, _ = decode_message(self.read_message())
        if isinstance(r, FailureMessage):
            raise r
        self.fields = r.data.get('fields', [])

        results = []
        m = PullAllMessage()
        self.send_message(m.encode())
        while True:
            r, _ = decode_message(self.read_message())
            if isinstance(r, FailureMessage):
                raise r
            if isinstance(r, SuccessMessage):
                break
            results.append(r.data)
        return results

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None


def nodes(rs):
    "Filter nodes from resultsets"
    results = []
    for r in rs:
        for e in r:
            if isinstance(e, Node):
                results.append(e)
    return results


def relationships(rs):
    "Filter relationships from resultsets"
    results = []
    for r in rs:
        for e in r:
            if isinstance(e, Relationship):
                results.append(e)
    return results


def paths(rs):
    "Filter paths from resultsets"
    results = []
    for r in rs:
        for e in r:
            if isinstance(e, Path):
                results.append(e)
    return results


def to_nxgraph(rs):
    "Convert resultsets to NetworkX graph"
    import networkx as nx
    G = nx.MultiDiGraph()
    for e in nodes(rs):
        d = {'labels': e.labels}
        d.update(e.properties)
        G.add_node(e.nodeIdentity, **d)

    for e in relationships(rs):
        d = {'typeName': e.typeName}
        d.update(e.properties)
        G.add_edge(e.startNodeIdentity, e.endNodeIdentity, **d)

    for p in paths(rs):
        for e in p.nodes():
            d = {'labels': e.labels}
            d.update(e.properties)
            G.add_node(e.nodeIdentity, **d)
        for e in p.relationships():
            d = {'typeName': e.typeName}
            d.update(e.properties)
            G.add_edge(e.startNodeIdentity, e.endNodeIdentity, **d)

    return G


def connect(host, user, password, port=7687):
    return BoltSession(host, user, password, port)
