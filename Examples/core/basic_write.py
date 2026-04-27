#!/usr/bin/env python3
"""
basic_write.py - is a functional example showing the simplest ways
in which to write data to an influxdb3 database using Point objects.

After successfully running this example try basic_query.py to verify the results.
"""
import datetime
import os

from influxdb_client_3 import InfluxDBClient3, Point

HOST = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
TOKEN = os.getenv('INFLUXDB_TOKEN') or 'my-token'
DATABASE = os.getenv('INFLUXDB_DATABASE') or 'my-db'

client = InfluxDBClient3(
    token=TOKEN,
    host=HOST,
    database=DATABASE,)

now = datetime.datetime.now(datetime.timezone.utc)

measurement = "basic_caught"

data = Point(measurement).tag("trainer", "ash").tag("id", "0006").tag("num", "1") \
    .field("caught", "charizard") \
    .field("level", 10).field("attack", 30) \
    .field("defense", 40).field("hp", 200) \
    .field("speed", 10) \
    .field("type1", "fire").field("type2", "flying") \
    .time(now)

try:
    client.write(data)
    print("First point written to InfluxDB!")
except Exception as e:
    print(f"Error writing point: {e}")

data = [Point(measurement)  # first point
        .tag("trainer", "ash")
        .tag("id", "0006")
        .tag("num", "1")
        .field("caught", "charizard")
        .field("level", 10)
        .field("attack", 30)
        .field("defense", 40)
        .field("hp", 200)
        .field("speed", 10)
        .field("type1", "fire")
        .field("type2", "flying")
        .time(now),

        Point(measurement)  # second point
        .tag("trainer", "ash")
        .tag("id", "0007")
        .tag("num", "2")
        .field("caught", "bulbasaur")
        .field("level", 12)
        .field("attack", 31)
        .field("defense", 31)
        .field("hp", 190)
        .field("speed", 11)
        .field("type1", "grass")
        .field("type2", "poison")
        .time(now),

        Point(measurement)  # third point
        .tag("trainer", "ash")
        .tag("id", "0008")
        .tag("num", "3")
        .field("caught", "squirtle")
        .field("level", 13)
        .field("attack", 29)
        .field("defense", 40)
        .field("hp", 180)
        .field("speed", 13)
        .field("type1", "water")
        .field("type2", None)
        .time(now)
        ]

try:
    client.write(data)
    print(f"Write success: {len(data)} points!")
except Exception as e:
    print(f"Error writing point: {e}")
finally:
    client.close()
