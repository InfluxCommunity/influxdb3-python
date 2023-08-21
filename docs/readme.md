
# Arrow Flight Error Guide

This guide provides details on errors returned by Arrow Flight, along with potential reasons for each error. If you encounter an error not listed here, please raise an issue or reach out for assistance.

## Table of Contents

- [Arrow Flight Error Guide](#arrow-flight-error-guide)
  - [Table of Contents](#table-of-contents)
  - [Errors](#errors)
    - [Internal Error: Received RST\_STREAM](#internal-error-received-rst_stream)
    - [Internal Error: stream terminated by RST\_STREAM with NO\_ERROR](#internal-error-stream-terminated-by-rst_stream-with-no_error)
    - [ArrowInvalid: Flight returned invalid argument error with message: bucket "" not found](#arrowinvalid-flight-returned-invalid-argument-error-with-message-bucket--not-found)
  - [Contributions](#contributions)

## Errors

### Internal Error: Received RST_STREAM

**Error Message:** 
`Flight returned internal error, with message: Received RST_STREAM with error code 2. gRPC client debug context: UNKNOWN:Error received from peer ipv4:34.196.233.7:443 {grpc_message:"Received RST_STREAM with error code 2"}`

**Potential Reasons:**
- The connection to the server was reset unexpectedly.
- Network issues between the client and server.
- Server might have closed the connection due to an internal error.
- The client exceeded the server's maximum number of concurrent streams.

### Internal Error: stream terminated by RST_STREAM with NO_ERROR

**Error Message:**
`pyarrow._flight.FlightInternalError: Flight returned internal error, with message: stream terminated by RST_STREAM with error code: NO_ERROR. gRPC client debug context: UNKNOWN:Error received from peer ipv4:3.123.149.45:443 {created_time:"2023-07-26T14:12:44.992317+02:00", grpc_status:13, grpc_message:"stream terminated by RST_STREAM with error code: NO_ERROR"}. Client context: OK`

**Potential Reasons:**
- The server terminated the stream, but there wasn't any specific error associated with it.
- Possible network disruption, even if it's temporary.
- The server might have reached its maximum capacity or other internal limits.
- Unspecified server-side issues that led to the termination of the stream.

### ArrowInvalid: Flight returned invalid argument error with message: bucket "" not found

**Error Message:**
`ArrowInvalid: Flight returned invalid argument error, with message: bucket "otel5" not found. gRPC client debug context: UNKNOWN:Error received from peer ipv4:3.123.149.45:443 {grpc_message:"bucket \"otel5\" not found", grpc_status:3, created_time:"2023-08-09T16:37:30.093946+01:00"}. Client context: IOError: Server never sent a data message. Detail: Internal`

**Potential Reasons:**
- The database has not been created within the current InfluxDB instance.


## Contributions

We welcome contributions to this guide. If you've encountered an Arrow Flight error not listed here, please raise an issue or submit a pull request.
