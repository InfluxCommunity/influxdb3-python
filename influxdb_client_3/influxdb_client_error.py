class InfluxDBClientError(Exception):
    pass


class InfluxdbClientQueryError(InfluxDBClientError):
    def __init__(self, error_message, *args, **kwargs):
        super().__init__(error_message, *args, **kwargs)
        self.message = error_message


class InfluxdbClientWriteError(InfluxDBClientError):
    pass
