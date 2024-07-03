"""
Functions for serialize Polars DataFrame.

Much of the code here is inspired by that in the aioinflux packet found here: https://github.com/gusutabopb/aioinflux
"""

import logging
import math

from influxdb_client_3.write_client.client.write.point import _ESCAPE_KEY, _ESCAPE_STRING, DEFAULT_WRITE_PRECISION

logger = logging.getLogger('influxdb_client.client.write.polars_dataframe_serializer')


class PolarsDataframeSerializer:
    """Serialize DataFrame into LineProtocols."""

    def __init__(self, data_frame, point_settings, precision=DEFAULT_WRITE_PRECISION, chunk_size: int = None,
                 **kwargs) -> None:
        """
        Init serializer.

        :param data_frame: Polars DataFrame to serialize
        :param point_settings: Default Tags
        :param precision: The precision for the unix timestamps within the body line-protocol.
        :param chunk_size: The size of chunk for serializing into chunks.
        :key data_frame_measurement_name: name of measurement for writing Polars DataFrame
        :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
        :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp.
        :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column
        """

        self.data_frame = data_frame
        self.point_settings = point_settings
        self.precision = precision
        self.chunk_size = chunk_size
        self.measurement_name = kwargs.get("data_frame_measurement_name", "measurement")
        self.tag_columns = kwargs.get("data_frame_tag_columns", [])
        self.timestamp_column = kwargs.get("data_frame_timestamp_column", None)
        self.timestamp_timezone = kwargs.get("data_frame_timestamp_timezone", None)

        self.column_indices = {name: index for index, name in enumerate(data_frame.columns)}

        if self.timestamp_column is None or self.timestamp_column not in self.column_indices:
            raise ValueError(
                f"Timestamp column {self.timestamp_column} not found in DataFrame. Please define a valid timestamp "
                f"column.")

        #
        # prepare chunks
        #
        if chunk_size is not None:
            self.number_of_chunks = int(math.ceil(len(data_frame) / float(chunk_size)))
            self.chunk_size = chunk_size
        else:
            self.number_of_chunks = None

    def escape_key(self, value):
        return str(value).translate(_ESCAPE_KEY)

    def escape_value(self, value):
        return str(value).translate(_ESCAPE_STRING)

    def to_line_protocol(self, row):
        # Filter out None or empty values for tags
        tags = ""

        tags = ",".join(
            f'{self.escape_key(col)}={self.escape_key(row[self.column_indices[col]])}'
            for col in self.tag_columns
            if row[self.column_indices[col]] is not None and row[self.column_indices[col]] != ""
        )

        if self.point_settings.defaultTags:
            default_tags = ",".join(
                f'{self.escape_key(key)}={self.escape_key(value)}'
                for key, value in self.point_settings.defaultTags.items()
            )
            # Ensure there's a comma between existing tags and default tags if both are present
            if tags and default_tags:
                tags += ","
            tags += default_tags

        # add escape symbols for special characters to tags

        fields = ",".join(
            f"{col}=\"{self.escape_value(row[self.column_indices[col]])}\"" if isinstance(row[self.column_indices[col]],
                                                                                          str)
            else f"{col}={str(row[self.column_indices[col]]).lower()}" if isinstance(row[self.column_indices[col]],
                                                                                     bool)  # Check for bool first
            else f"{col}={row[self.column_indices[col]]}i" if isinstance(row[self.column_indices[col]], int)
            else f"{col}={row[self.column_indices[col]]}"
            for col in self.column_indices
            if col not in self.tag_columns + [self.timestamp_column] and
            row[self.column_indices[col]] is not None and row[self.column_indices[col]] != ""
        )

        # Access the Unix timestamp
        timestamp = row[self.column_indices[self.timestamp_column]]
        if tags != "":
            line_protocol = f"{self.measurement_name},{tags} {fields} {timestamp}"
        else:
            line_protocol = f"{self.measurement_name} {fields} {timestamp}"

        return line_protocol

    def serialize(self, chunk_idx: int = None):
        import polars as pl

        df = self.data_frame

        # Check if the timestamp column is already an integer
        if df[self.timestamp_column].dtype in [pl.Int32, pl.Int64]:
            # The timestamp column is already an integer, assuming it's in Unix format
            pass
        else:
            # Convert timestamp to Unix timestamp based on specified precision
            if self.precision in [None, 'ns']:
                df = df.with_columns(
                    pl.col(self.timestamp_column).dt.epoch(time_unit="ns").alias(self.timestamp_column))
            elif self.precision == 'us':
                df = df.with_columns(
                    pl.col(self.timestamp_column).dt.epoch(time_unit="us").alias(self.timestamp_column))
            elif self.precision == 'ms':
                df = df.with_columns(
                    pl.col(self.timestamp_column).dt.epoch(time_unit="ms").alias(self.timestamp_column))
            elif self.precision == 's':
                df = df.with_columns(pl.col(self.timestamp_column).dt.epoch(time_unit="s").alias(self.timestamp_column))
            else:
                raise ValueError(f"Unsupported precision: {self.precision}")

        if chunk_idx is None:
            chunk = df
        else:
            logger.debug("Serialize chunk %s/%s ...", chunk_idx + 1, self.number_of_chunks)
            chunk = df[chunk_idx * self.chunk_size:(chunk_idx + 1) * self.chunk_size]

        # Apply the UDF to each row
        line_protocol_expr = chunk.map_rows(self.to_line_protocol, return_dtype=pl.Object)

        lp = line_protocol_expr['map'].to_list()

        return lp


def polars_data_frame_to_list_of_points(data_frame, point_settings, precision=DEFAULT_WRITE_PRECISION, **kwargs):
    """
    Serialize DataFrame into LineProtocols.

    :param data_frame: Pandas DataFrame to serialize
    :param point_settings: Default Tags
    :param precision: The precision for the unix timestamps within the body line-protocol.
    :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame
    :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
    :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp. The column can be defined as a :class:`~str` value
                                      formatted as `2018-10-26`, `2018-10-26 12:00`, `2018-10-26 12:00:00-05:00`
                                      or other formats and types supported by `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_ - ``DataFrame``
    :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column - ``DataFrame``
    """  # noqa: E501
    return PolarsDataframeSerializer(data_frame, point_settings, precision, **kwargs).serialize()
