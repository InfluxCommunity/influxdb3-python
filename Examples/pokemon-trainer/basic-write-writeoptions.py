import datetime

from influxdb_client_3 import InfluxDBClient3, Point, SYNCHRONOUS, write_client_options

wco = write_client_options(write_options=SYNCHRONOUS)

with InfluxDBClient3(
        token="",
        host="eu-central-1-1.aws.cloud2.influxdata.com",
        database="pokemon-codex",
        write_client_options=wco,
        debug=True) as client:
    now = datetime.datetime.now(datetime.timezone.utc)

    data = Point("caught").tag("trainer", "ash").tag("id", "0006").tag("num", "1") \
        .field("caught", "charizard") \
        .field("level", 10).field("attack", 30) \
        .field("defense", 40).field("hp", 200) \
        .field("speed", 10) \
        .field("type1", "fire").field("type2", "flying") \
        .time(now)

    try:
        client.write(data)
    except Exception as e:
        print(f"Error writing point: {e}")

    data = []
    # Adding first point
    data.append(
        Point("caught")
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
        .time(now)
    )

    # Adding second point
    data.append(
        Point("caught")
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
        .time(now)
    )

    # Adding third point
    data.append(
        Point("caught")
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
    )

    try:
        client.write(data)
    except Exception as e:
        print(f"Error writing point: {e}")
