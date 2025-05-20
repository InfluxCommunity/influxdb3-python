import json
import struct
import time

from pyarrow import (
    array,
    Table,
    concat_tables, ArrowException
)
from pyarrow.flight import (
    FlightServerBase,
    RecordBatchStream,
    ServerMiddlewareFactory,
    FlightUnauthenticatedError,
    ServerMiddleware,
    GeneratorStream,
    ServerAuthHandler
)


class NoopAuthHandler(ServerAuthHandler):
    """A no-op auth handler - as seen in pyarrow tests"""

    def authenticate(self, outgoing, incoming):
        """Do nothing"""

    def is_valid(self, token):
        """
        Return an empty string
        N.B. Returning None causes Type error
        :param token:
        :return:
        """
        return ""


def case_insensitive_header_lookup(headers, lkey):
    """Lookup the value of a given key in the given headers.
       The lkey is case-insensitive.
    """
    for key in headers:
        if key.lower() == lkey.lower():
            return headers.get(key)


req_headers = {}


def set_req_headers(headers):
    global req_headers
    req_headers = headers


def get_req_headers():
    global req_headers
    return req_headers


class ConstantData:

    def __init__(self):
        self.data = [
            array(['temp', 'temp', 'temp']),
            array(['kitchen', 'common', 'foyer']),
            array([36.9, 25.7, 9.8])
        ]
        self.names = ['data', 'reference', 'value']

    def to_tuples(self):
        response = []
        for n in range(3):
            response.append((self.data[0][n].as_py(), self.data[1][n].as_py(), self.data[2][n].as_py()))
        return response

    def to_list(self):
        response = []
        for it in range(len(self.data[0])):
            item = {}
            for o in range(len(self.names)):
                item[self.names[o]] = self.data[o][it].as_py()
            response.append(item)
        return response


class ConstantFlightServer(FlightServerBase):

    def __init__(self, location=None, options=None, **kwargs):
        super().__init__(location, **kwargs)
        self.cd = ConstantData()
        self.options = options

    # respond with Constant Data plus fields from ticket
    def do_get(self, context, ticket):
        result_table = Table.from_arrays(self.cd.data, names=self.cd.names)
        tkt = json.loads(ticket.ticket.decode('utf-8'))
        for key in tkt.keys():
            tkt_data = [
                array([key]),
                array([tkt[key]]),
                array([-1.0])
            ]
            result_table = concat_tables([result_table, Table.from_arrays(tkt_data, names=self.cd.names)])
        return RecordBatchStream(result_table, options=self.options)


class ConstantFlightServerDelayed(ConstantFlightServer):

    def __init__(self, location=None, options=None, delay=0.5, **kwargs):
        super().__init__(location, **kwargs)
        self.delay = delay

    def do_get(self, context, ticket):
        time.sleep(self.delay)
        return super().do_get(context, ticket)


class HeaderCheckServerMiddlewareFactory(ServerMiddlewareFactory):
    """Factory to create HeaderCheckServerMiddleware and check header values"""
    def start_call(self, info, headers):
        auth_header = case_insensitive_header_lookup(headers, "Authorization")
        values = auth_header[0].split(' ')
        if values[0] != 'Bearer':
            raise FlightUnauthenticatedError("Token required")
        global req_headers
        req_headers = headers
        return HeaderCheckServerMiddleware(values[1])


class HeaderCheckServerMiddleware(ServerMiddleware):
    """
    Middleware needed to catch request headers via factory
    N.B. As found in pyarrow tests
    """
    def __init__(self, token, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token

    def sending_headers(self):
        return {'authorization': 'Bearer ' + self.token}


class HeaderCheckFlightServer(FlightServerBase):
    """Mock server handle gRPC do_get calls"""
    def do_get(self, context, ticket):
        """Return something to avoid needless errors"""
        data = [
            array([b"Vltava", struct.pack('<i', 105), b"FM"])
        ]
        table = Table.from_arrays(data, names=['a'])
        return GeneratorStream(
            table.schema,
            self.number_batches(table),
            options={})

    @staticmethod
    def number_batches(table):
        for idx, batch in enumerate(table.to_batches()):
            buf = struct.pack('<i', idx)
            yield batch, buf

class ErrorFlightServer(FlightServerBase):
    def do_get(self, context, ticket):
        raise ArrowException