import asyncio
import random
import time

import pandas

from influxdb_client_3 import InfluxDBClient3

from config import Config


async def fibio(iterations, grit=0.5):
    """
    example coroutine to run parallel with query_async
    :param iterations:
    :param grit:
    :return:
    """
    n0 = 1
    n1 = 1
    vals = [n0, n1]
    for _ in range(iterations):
        val = n0 + n1
        n0 = n1
        n1 = val
        print(val)
        vals.append(val)
        await asyncio.sleep(grit)
    return vals


def write_data(client: InfluxDBClient3, measurement):
    """
    Synchronous write - only for preparing data
    :param client:
    :param measurement:
    :return:
    """
    ids = ['s3b1', 'dq41', 'sgw22']
    lp_template = f"{measurement},id=%s speed=%f,alt=%f,bearing=%f %d"
    data_size = 10
    data = []
    interval = 10 * 1_000_000_000
    ts = time.time_ns() - (interval * data_size)
    for _ in range(data_size):
        data.append(lp_template % (ids[random.randint(0, len(ids) - 1)],
                                   random.random() * 300,
                                   random.random() * 2000,
                                   random.random() * 360, ts))
        ts += interval

    client.write(data)


async def query_data(client: InfluxDBClient3, measurement):
    """
    Query asynchronously - should not block other coroutines
    :param client:
    :param measurement:
    :return:
    """
    query = f"SELECT * FROM \"{measurement}\" WHERE time >= now() - interval '5 minutes' ORDER BY time DESC"
    print(f"query start:    {pandas.Timestamp(time.time_ns())}")
    table = await client.query_async(query)
    print(f"query returned: {pandas.Timestamp(time.time_ns())}")
    return table.to_pandas()


async def main():
    config = Config()
    client = InfluxDBClient3(
        host=config.host,
        token=config.token,
        database=config.database,
        org=config.org
    )
    measurement = 'example_uav'
    write_data(client, measurement)

    # run both coroutines simultaneously
    result = await asyncio.gather(fibio(10, 0.2), query_data(client, measurement))
    print(f"fibio sequence = {result[0]}")
    print(f"data set =\n{result[1]}")


if __name__ == "__main__":
    asyncio.run(main())
