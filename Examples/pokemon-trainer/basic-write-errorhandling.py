import datetime

from influxdb_client_3 import InfluxDBClient3, Point, SYNCHRONOUS, write_client_options

wco = write_client_options(write_options=SYNCHRONOUS)

with InfluxDBClient3(
        token="",
        host="eu-central-1-1.aws.cloud2.influxdata.com",
        org="6a841c0c08328fb1",
        database="pokemon-codex", write_client_options=wco) as client:
    now = datetime.datetime.now(datetime.timezone.utc)

    data = Point("caught").tag("trainer", "ash").tag("id", "0006").tag("num", "1") \
        .field("caught", "charizard") \
        .field("level", 10).field("attack", 30) \
        .field("defense", 40).field("hp", 200) \
        .field("speed", 10) \
        .field("type1", "fire").field("type2", "flying") \
        .time(now)

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

    # Bad point
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

    # Good Query
    try:
        table = client.query(query='''SELECT * FROM "caught" WHERE time > now() - 5m''', language='influxql')
        print(table)
    except Exception as e:
        print(f"Error querying data: {e}")

    # Bad Query - not a sql query
    try:
        table = client.query(query='''SELECT * FROM "caught" WHERE time > now() - 5m''', language='sql')
        print(table)
    except Exception as e:
        print(f"Error querying data: {e}")
