import unittest
from datetime import timedelta
from io import StringIO

import numpy as np
import pandas as pd

from influxdb_client_3 import PointSettings, WritePrecision
from influxdb_client_3.write_client.client.write.dataframe_serializer import DataframeSerializer, \
    data_frame_to_list_of_points


class TestDataFrameSerializer(unittest.TestCase):

    def test_nullable_types(self):
        df = pd.DataFrame({
            "bool_nulls": [True, None, False],
            "int_nulls": [None, 1, 2],
            "float_nulls": [1.0, 2.0, None],
            "str_nulls": ["a", "b", None],
        })
        df['bool_nulls_pd'] = df['bool_nulls'].astype(pd.BooleanDtype())
        df['int_nulls_pd'] = df['int_nulls'].astype(pd.Int64Dtype())
        df['float_nulls_pd'] = df['float_nulls'].astype(pd.Float64Dtype())
        df['str_nulls_pd'] = df['str_nulls'].astype(pd.StringDtype())

        df.index = pd.to_datetime(["2021-01-01", "2021-01-02", "2021-01-03"])

        ps = PointSettings()

        serializer = DataframeSerializer(df, ps, data_frame_measurement_name="test")

        lines = serializer.serialize()

        # make sure there are no `<NA>` values in the serialized lines
        # first line should not have "int"
        first_line = lines[0]
        self.assertNotIn('<NA>', first_line)
        self.assertNotIn('int_nulls', first_line)

        # the second line should not have "bool"
        second_line = lines[1]
        self.assertNotIn('<NA>', second_line)
        self.assertNotIn('bool_nulls', second_line)

        # the third line should not have "str" or "float"
        third_line = lines[2]
        self.assertNotIn('<NA>', third_line)
        self.assertNotIn('str_nulls', third_line)
        self.assertNotIn('float_nulls', third_line)

    def test_null_and_inf_values(self):
        df = pd.DataFrame({
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": pd.Series([72.3, -np.inf, np.inf]).astype(pd.Float64Dtype()),
            "time": pd.to_datetime(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .astype('datetime64[s, UTC]'),
        })
        ps = PointSettings()
        actual = data_frame_to_list_of_points(df, ps,
                                              data_frame_measurement_name='iot-devices',
                                              data_frame_tag_columns=['building'],
                                              data_frame_timestamp_column='time')

        expected = [
            'iot-devices,building=5a name="iot-devices",temperature=72.3 1664625660000000000',
            'iot-devices,building=5a name="iot-devices" 1664712060000000000',
            'iot-devices,building=5a name="iot-devices" 1664798460000000000'
        ]
        self.assertEqual(expected, actual)

    def test_to_list_of_points(self):
        df = pd.DataFrame({
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": [72.3, 72.1, 72.2],
            "time": pd.to_datetime(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .astype('datetime64[s, UTC]'),
        })
        ps = PointSettings()
        actual = data_frame_to_list_of_points(df, ps,
                                              data_frame_measurement_name='iot-devices',
                                              data_frame_tag_columns=['building'],
                                              data_frame_timestamp_column='time')

        expected = [
            'iot-devices,building=5a name="iot-devices",temperature=72.3 1664625660000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.1 1664712060000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.2 1664798460000000000'
        ]
        self.assertEqual(expected, actual)

    def test_write_nan(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
            [3.1955, np.nan, 20.514305, np.nan],
            [5.7310, np.nan, 23.328710, np.nan],
            [np.nan, 3.138664, np.nan, 20.755026],
            [5.7310, 5.139563, 23.328710, 19.791240],
            [np.nan, np.nan, np.nan, np.nan],
        ],
            index=[now, now + timedelta(minutes=30), now + timedelta(minutes=60),
                   now + timedelta(minutes=90), now + timedelta(minutes=120)],
            columns=["actual_kw_price", "forecast_kw_price", "actual_general_use",
                     "forecast_general_use"])

        points = data_frame_to_list_of_points(data_frame=data_frame, point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(4, len(points))
        self.assertEqual("measurement actual_general_use=20.514305,actual_kw_price=3.1955 1586044800000000000",
                         points[0])
        self.assertEqual("measurement actual_general_use=23.32871,actual_kw_price=5.731 1586046600000000000",
                         points[1])
        self.assertEqual("measurement forecast_general_use=20.755026,forecast_kw_price=3.138664 1586048400000000000",
                         points[2])
        self.assertEqual("measurement actual_general_use=23.32871,actual_kw_price=5.731,forecast_general_use=19.79124"
                         ",forecast_kw_price=5.139563 1586050200000000000",
                         points[3])

    def test_write_tag_nan(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
            ["", 3.1955, 20.514305],
            ['', 5.7310, 23.328710],
            [np.nan, 5.7310, 23.328710],
            ["tag", 3.138664, 20.755026],
        ],
            index=[now, now + timedelta(minutes=30),
                   now + timedelta(minutes=60), now + timedelta(minutes=90)],
            columns=["tag", "actual_kw_price", "forecast_kw_price"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(4, len(points))
        self.assertEqual("measurement actual_kw_price=3.1955,forecast_kw_price=20.514305 1586044800000000000",
                         points[0])
        self.assertEqual("measurement actual_kw_price=5.731,forecast_kw_price=23.32871 1586046600000000000",
                         points[1])
        self.assertEqual("measurement actual_kw_price=5.731,forecast_kw_price=23.32871 1586048400000000000",
                         points[2])
        self.assertEqual("measurement,tag=tag actual_kw_price=3.138664,forecast_kw_price=20.755026 1586050200000000000",
                         points[3])

    def test_write_object_field_nan(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
            ["foo", 1],
            [np.nan, 2],
        ],
            index=[now, now + timedelta(minutes=30)],
            columns=["obj", "val"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(2, len(points))
        self.assertEqual("measurement obj=\"foo\",val=1i 1586044800000000000",
                         points[0])
        self.assertEqual("measurement val=2i 1586046600000000000",
                         points[1])

    def test_write_missing_values(self):
        data_frame = pd.DataFrame({
            "a_bool": [True, None, False],
            "b_int": [None, 1, 2],
            "c_float": [1.0, 2.0, None],
            "d_str": ["a", "b", None],
        })

        data_frame['a_bool'] = data_frame['a_bool'].astype(pd.BooleanDtype())
        data_frame['b_int'] = data_frame['b_int'].astype(pd.Int64Dtype())
        data_frame['c_float'] = data_frame['c_float'].astype(pd.Float64Dtype())
        data_frame['d_str'] = data_frame['d_str'].astype(pd.StringDtype())

        print(data_frame)
        points = data_frame_to_list_of_points(
            data_frame=data_frame,
            point_settings=PointSettings(),
            data_frame_measurement_name='measurement')

        self.assertEqual(3, len(points))
        self.assertEqual("measurement a_bool=True,c_float=1.0,d_str=\"a\" 0", points[0])
        self.assertEqual("measurement b_int=1i,c_float=2.0,d_str=\"b\" 1", points[1])
        self.assertEqual("measurement a_bool=False,b_int=2i 2", points[2])

    def test_write_field_bool(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
            [True],
            [False],
        ],
            index=[now, now + timedelta(minutes=30)],
            columns=["status"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measurement')

        self.assertEqual(2, len(points))
        self.assertEqual("measurement status=True 1586044800000000000",
                         points[0])
        self.assertEqual("measurement status=False 1586046600000000000",
                         points[1])

    def test_escaping_measurement(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[
            ["coyote_creek", np.int64(100.5)],
            ["coyote_creek", np.int64(200)],
        ],
            index=[now + timedelta(hours=1), now + timedelta(hours=2)],
            columns=["location", "water_level"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measu rement',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(2, len(points))
        self.assertEqual("measu\\ rement location=\"coyote_creek\",water_level=100i 1586048400000000000",
                         points[0])
        self.assertEqual("measu\\ rement location=\"coyote_creek\",water_level=200i 1586052000000000000",
                         points[1])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='measu\nrement2',
                                              data_frame_tag_columns={"tag"})

        self.assertEqual(2, len(points))
        self.assertEqual("measu\\nrement2 location=\"coyote_creek\",water_level=100i 1586048400000000000",
                         points[0])
        self.assertEqual("measu\\nrement2 location=\"coyote_creek\",water_level=200i 1586052000000000000",
                         points[1])

    def test_tag_escaping_key_and_value(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["carriage\nreturn", "new\nline", "t\tab", np.int64(2)], ],
                                  index=[now + timedelta(hours=1), ],
                                  columns=["carriage\rreturn", "new\nline", "t\tab", "l\ne\rv\tel"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h\n2\ro\t_data',
                                              data_frame_tag_columns={"new\nline", "carriage\rreturn", "t\tab"})

        self.assertEqual(1, len(points))
        self.assertEqual(
            "h\\n2\\ro\\t_data,carriage\\rreturn=carriage\\nreturn,new\\nline=new\\nline,t\\tab=t\\tab l\\ne\\rv\\tel=2i 1586048400000000000",  # noqa: E501
            points[0])

    def test_tags_order(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        data_frame = pd.DataFrame(data=[["c", "a", "b", np.int64(2)], ],
                                  index=[now + timedelta(hours=1), ],
                                  columns=["c", "a", "b", "level"])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h2o',
                                              data_frame_tag_columns={"c", "a", "b"})

        self.assertEqual(1, len(points))
        self.assertEqual("h2o,a=a,b=b,c=c level=2i 1586048400000000000", points[0])

    def test_escape_text_value(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')
        an_hour_ago = now - timedelta(hours=1)

        test = [{'a': an_hour_ago, 'b': 'hello world', 'c': 1, 'd': 'foo bar'},
                {'a': now, 'b': 'goodbye cruel world', 'c': 2, 'd': 'bar foo'}]

        data_frame = pd.DataFrame(test)
        data_frame = data_frame.set_index('a')

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='test',
                                              data_frame_tag_columns=['d'])

        self.assertEqual(2, len(points))
        self.assertEqual("test,d=foo\\ bar b=\"hello world\",c=1i 1586041200000000000", points[0])
        self.assertEqual("test,d=bar\\ foo b=\"goodbye cruel world\",c=2i 1586044800000000000", points[1])

    def test_with_period_index(self):
        data_frame = pd.DataFrame(data={
            'value': [1, 2],
        },
            index=pd.period_range(start='2020-04-05 01:00', freq='h', periods=2))

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              point_settings=PointSettings(),
                                              data_frame_measurement_name='h2o')

        self.assertEqual(2, len(points))
        self.assertEqual("h2o value=1i 1586048400000000000", points[0])
        self.assertEqual("h2o value=2i 1586052000000000000", points[1])

    def test_write_num_py_floats(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')

        float_types = [np.float16, np.float32, np.float64]
        if hasattr(np, 'float128'):
            float_types.append(np.float128)
        for np_float_type in float_types:
            data_frame = pd.DataFrame([15.5], index=[now], columns=['level']).astype(np_float_type)
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings())
            self.assertEqual(1, len(points))
            self.assertEqual("h2o level=15.5 1586044800000000000", points[0], msg=f'Current type: {np_float_type}')

    def test_write_precision(self):
        now = pd.Timestamp('2020-04-05 00:00+00:00')
        precisions = [
            (WritePrecision.NS, 1586044800000000000),
            (WritePrecision.US, 1586044800000000),
            (WritePrecision.MS, 1586044800000),
            (WritePrecision.S, 1586044800),
            (None, 1586044800000000000)
        ]

        for precision in precisions:
            data_frame = pd.DataFrame([15], index=[now], columns=['level'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings(),
                                                  precision=precision[0])
            self.assertEqual(1, len(points))
            self.assertEqual(f"h2o level=15i {precision[1]}", points[0])

    def test_index_not_periodIndex_respect_write_precision(self):
        precisions = [
            (WritePrecision.NS, 1586044800000000000),
            (WritePrecision.US, 1586044800000000),
            (WritePrecision.MS, 1586044800000),
            (WritePrecision.S, 1586044800),
            (None, 1586044800000000000)
        ]

        for precision in precisions:
            data_frame = pd.DataFrame([15], index=[precision[1]], columns=['level'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name='h2o',
                                                  point_settings=PointSettings(),
                                                  precision=precision[0])
            self.assertEqual(1, len(points))
            self.assertEqual(f"h2o level=15i {precision[1]}", points[0])

    def test_serialize_strings_with_commas(self):
        csv = StringIO("""sep=;
Date;Entry Type;Value;Currencs;Category;Person;Account;Counter Account;Group;Note;Recurring;
"01.10.2018";"Expense";"-1,00";"EUR";"Testcategory";"";"Testaccount";"";"";"This, works";"no";
"02.10.2018";"Expense";"-1,00";"EUR";"Testcategory";"";"Testaccount";"";"";"This , works not";"no";
""")
        data_frame = pd.read_csv(csv, sep=";", skiprows=1, decimal=",", encoding="utf-8")
        data_frame['Date'] = pd.to_datetime(data_frame['Date'], format="%d.%m.%Y")
        data_frame.set_index('Date', inplace=True)

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="bookings",
                                              data_frame_tag_columns=['Entry Type', 'Category', 'Person', 'Account'],
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual("bookings,Account=Testaccount,Category=Testcategory,Entry\\ Type=Expense Currencs=\"EUR\",Note=\"This, works\",Recurring=\"no\",Value=-1.0 1538352000000000000", points[0])  # noqa: E501
        self.assertEqual("bookings,Account=Testaccount,Category=Testcategory,Entry\\ Type=Expense Currencs=\"EUR\",Note=\"This , works not\",Recurring=\"no\",Value=-1.0 1538438400000000000", points[1])  # noqa: E501

    def test_without_tags_and_fields_with_nan(self):
        df = pd.DataFrame({
            'a': np.arange(0., 3.),
            'b': [0., np.nan, 1.],
        }).set_index(pd.to_datetime(['2021-01-01 0:00', '2021-01-01 0:01', '2021-01-01 0:02']))

        points = data_frame_to_list_of_points(data_frame=df,
                                              data_frame_measurement_name="test",
                                              point_settings=PointSettings())

        self.assertEqual(3, len(points))
        self.assertEqual("test a=0.0,b=0.0 1609459200000000000", points[0])
        self.assertEqual("test a=1.0 1609459260000000000", points[1])
        self.assertEqual("test a=2.0,b=1.0 1609459320000000000", points[2])

    def test_use_timestamp_from_specified_column(self):
        data_frame = pd.DataFrame(data={
            'column_time': ['2020-04-05', '2020-05-05'],
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=['A', 'B'])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_column="column_time",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1586044800000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1588636800000000000', points[1])

    def test_str_format_for_timestamp(self):
        time_formats = [
            ('2018-10-26', 'test value1=10i,value2=20i 1540512000000000000'),
            ('2018-10-26 10:00', 'test value1=10i,value2=20i 1540548000000000000'),
            ('2018-10-26 10:00:00-05:00', 'test value1=10i,value2=20i 1540566000000000000'),
            ('2018-10-26T11:00:00+00:00', 'test value1=10i,value2=20i 1540551600000000000'),
            ('2018-10-26 12:00:00+00:00', 'test value1=10i,value2=20i 1540555200000000000'),
            ('2018-10-26T16:00:00-01:00', 'test value1=10i,value2=20i 1540573200000000000'),
        ]

        for time_format in time_formats:
            data_frame = pd.DataFrame(data={
                'column_time': [time_format[0]],
                'value1': [10],
                'value2': [20],
            }, index=['A'])
            points = data_frame_to_list_of_points(data_frame=data_frame,
                                                  data_frame_measurement_name="test",
                                                  data_frame_timestamp_column="column_time",
                                                  point_settings=PointSettings())

            self.assertEqual(1, len(points))
            self.assertEqual(time_format[1], points[0])

    def test_specify_timezone(self):
        data_frame = pd.DataFrame(data={
            'column_time': ['2020-05-24 10:00', '2020-05-24 01:00'],
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=['A', 'B'])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_column="column_time",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590274800000000000', points[1])

    def test_specify_timezone_date_time_index(self):
        data_frame = pd.DataFrame(data={
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=[pd.Timestamp('2020-05-24 10:00'), pd.Timestamp('2020-05-24 01:00')])

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590274800000000000', points[1])

    def test_specify_timezone_period_time_index(self):
        data_frame = pd.DataFrame(data={
            'value1': [10, 20],
            'value2': [30, 40],
        }, index=pd.period_range(start='2020-05-24 10:00', freq='h', periods=2))

        print(data_frame.to_string())

        points = data_frame_to_list_of_points(data_frame=data_frame,
                                              data_frame_measurement_name="test",
                                              data_frame_timestamp_timezone="Europe/Berlin",
                                              point_settings=PointSettings())

        self.assertEqual(2, len(points))
        self.assertEqual('test value1=10i,value2=30i 1590307200000000000', points[0])
        self.assertEqual('test value1=20i,value2=40i 1590310800000000000', points[1])

    def test_serialization_for_nan_in_columns_starting_with_digits(self):
        data_frame = pd.DataFrame(data={
            '1value': [np.nan, 30.0, np.nan, 30.0, np.nan],
            '2value': [30.0, np.nan, np.nan, np.nan, np.nan],
            '3value': [30.0, 30.0, 30.0, np.nan, np.nan],
            'avalue': [30.0, 30.0, 30.0, 30.0, 30.0]
        }, index=pd.period_range('2020-05-24 10:00', freq='h', periods=5))

        points = data_frame_to_list_of_points(data_frame,
                                              PointSettings(),
                                              data_frame_measurement_name='test')

        self.assertEqual(5, len(points))
        self.assertEqual('test 2value=30.0,3value=30.0,avalue=30.0 1590314400000000000', points[0])
        self.assertEqual('test 1value=30.0,3value=30.0,avalue=30.0 1590318000000000000', points[1])
        self.assertEqual('test 3value=30.0,avalue=30.0 1590321600000000000', points[2])
        self.assertEqual('test 1value=30.0,avalue=30.0 1590325200000000000', points[3])
        self.assertEqual('test avalue=30.0 1590328800000000000', points[4])

        data_frame = pd.DataFrame(data={
            '1value': [np.nan],
            'avalue': [30.0],
            'bvalue': [30.0]
        }, index=pd.period_range('2020-05-24 10:00', freq='h', periods=1))

        points = data_frame_to_list_of_points(data_frame,
                                              PointSettings(),
                                              data_frame_measurement_name='test')
        self.assertEqual(1, len(points))
        self.assertEqual('test avalue=30.0,bvalue=30.0 1590314400000000000', points[0])
