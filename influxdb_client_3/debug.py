import pyarrow.flight as flight

class TracingClientMiddleWareFactory(flight.ClientMiddleware):
    def start_call(self, info):
        print("Starting new call:", info)
        return TracingClientMiddleware()

class TracingClientMiddleware(flight.ClientMiddleware):
    def sending_headers(self):
        print("Sending trace ID:", "traceheader")
        return {
            "x-tracing-id": "traceheader",
        }

    def received_headers(self, headers):
        if "trace-id" in headers:
            trace_id = headers["trace-id"][0]
            print("Found trace header with value:", trace_id)
            # Don't overwrite our trace ID