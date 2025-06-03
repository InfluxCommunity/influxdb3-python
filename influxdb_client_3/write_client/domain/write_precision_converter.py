from influxdb_client_3.write_client.domain import WritePrecision


class WritePrecisionConverter(object):

    @staticmethod
    def to_v2_api_string(precision):
        """
        Converts WritePrecision to its string representation for V2 API.
        """
        if precision in [WritePrecision.NS, WritePrecision.US, WritePrecision.MS, WritePrecision.S]:
            return precision
        else:
            raise ValueError("Unsupported precision '%s'" % precision)

    @staticmethod
    def to_v3_api_string(precision):
        """
        Converts WritePrecision to its string representation for V3 API.
        """
        if precision == WritePrecision.NS:
            return "nanosecond"
        elif precision == WritePrecision.US:
            return "microsecond"
        elif precision == WritePrecision.MS:
            return "millisecond"
        elif precision == WritePrecision.S:
            return "second"
        else:
            raise ValueError("Unsupported precision '%s'" % precision)
