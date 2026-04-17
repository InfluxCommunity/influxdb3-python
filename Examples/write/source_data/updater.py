import pandas as pd
import pyarrow.feather as feather
import time
from datetime import datetime, timedelta
import logging
import random
import numpy as np
import pyarrow as pa
import pyarrow.compute as pac

# TODO output as orc, json, csv etc.

def update_measurement_name(source: pd.DataFrame, measurment_name: str):
    count = 0
    for _ in source["iox::measurement"]:
        source.loc[count, "iox::measurement"] = measurment_name
        count += 1

def update_timestamps(source: pd.DataFrame):

    #  now = datetime.now()
    now = time.time_ns();
    interval = 333_000_000 # ms
    grit = random.randrange(0, 1_000_000)
    interval = interval + grit
    # current = now - (timedelta(milliseconds=interval) * len(source["time"]))
    current = np.int64(now - interval * len(source["time"]))
    print(f"DEBUG source['time'] {source['time'][0]} {type(source['time'][0]).__name__}")
    print(f"DEBUG source[\"time\"].dtype {source["time"].dtype}")
    ts_type = type(source['time'][0]).__name__

    count = 0
    for _ in source['time']:
        if ts_type in ["int", "int32", "int64"]:
            #print(f"current {current} as type {type(current).__name__}")
            source.loc[count, "time"] = current
            # source.at[count, "time"] = current
            # source.at[count, "time"].astype('int64')
            #print(f"DEBUG source.loc[count, \"time\"] {source.loc[count, "time"]} {type(source.loc[count, "time"]).__name__}")
            # count += 1
        else:
            # ts = pd.Timestamp(current)
            # print(f"{data} {type(data)} ts {ts}")
            source.loc[count, "time"] = pd.Timestamp(current)
        count += 1
        current = current + interval

    if ts_type in ["int", "int32", "int64"]:
        source["time"] = source["time"].astype("int64")

def feather_update():
    logging.info("Updating feather")
    f_df = pd.read_feather('./out.feather')
    update_timestamps(f_df)
    update_measurement_name(f_df, "machine_data_feather")
    f_df.to_feather('./out_update.feather')

def orc_update():
    logging.info("Updating orc")
    o_df = pd.read_orc('./out.orc')
    update_timestamps(o_df)
    update_measurement_name(o_df, "machine_data_orc")
    o_df.to_orc('./out_update.orc', index=False)

def parquet_update():
    logging.info("Updating parquet")
    p_df = pd.read_parquet('./out.parquet')
    update_timestamps(p_df)
    update_measurement_name(p_df, "machine_data_parquet")
    p_df.to_parquet('./out_update.parquet', index=False)

def csv_update():
    logging.info("Updating csv")
    csv_df = pd.read_csv('./out.csv')
    update_timestamps(csv_df)
    csv_df.to_csv('./out_update.csv', index=False)

def json_update():
    logging.info("Updating json")
    json_df = pd.read_json('./out.json')
    update_timestamps(json_df)
    print(f"DEBUG json_df[\"time\"][0] {json_df['time'][0]} as type {type(json_df['time'][0])}")
    json_df.to_json('./out_update.json', orient='records', index=False)

def explore():
    logging.info("Explore")
    read_frame = feather.read_feather('./out.feather')
    # print(f"DEBUG read_table:\n{read_table}")
    c = len(read_frame["time"])
    print(f"DEBUG c: {c}  type: {type(c)} \n")
    print(f"DEBUG read_frame length: {read_frame.count()} type: {type(read_frame.count())}")
    print(f"DEBUG read_frame:\n{read_frame}")

    now = datetime.now()
    interval = 334 # ms
    # current = now - (timedelta(milliseconds=interval) * read_frame.count())
    current = now - (timedelta(milliseconds=interval) * len(read_frame["time"]))

    count = 0
    # new_ts = []
    for data in read_frame['time']:
        print(f"{data} {type(data)} current {current}")
        read_frame.loc[count, "time"] = current
        # read_frame['time'][count] = data
        # new_ts.append(current)
        count += 1
        current = current + timedelta(milliseconds=interval)

    # for data in read_frame['time']:
    #    print(f"{data}" )

    print(f"DEBUG read_frame:\n{read_frame}")

    # as feather
    # feather.write_feather(read_frame, './out_test.feather')
    read_frame.to_feather('./out_test.feather')

    # as csv
    count = 0
    for _ in read_frame['iox::measurement']:
        read_frame.loc[count, "iox::measurement"] = "machine_data_csv"
        count += 1

    read_frame.to_csv('./out_test.csv', columns=[
        "iox::measurement","time","host","load","machineID","power","provider","temperature","topic","vibration"
    ], index=False)

    # as json
    read_frame.to_json('./out_test.json', orient='records',)
    # test_frame = read_frame.pivot(index='iox::measurement', columns='time', values=[
    #    'iox::measurement','host','load','machineID','power','provider','temperature','topic','vibration'
    # ])
    # test_frame = pd.pivot_table(read_frame,values="iox::measurement", index="time", aggfunc=None)

    # print(f"DEBUG test_frame:\n{test_frame}")
    # test_frame.to_json('./out_test3.json')

    # as orc
    read_frame.to_orc('./out_test.orc')

    test_frame = pd.read_csv('./out.csv')
    print(f"DEBUG test_frame:\n{test_frame}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    feather_update()
    orc_update()
    parquet_update()
    csv_update()
    json_update()
