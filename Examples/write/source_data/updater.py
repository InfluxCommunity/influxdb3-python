#!/usr/bin/env python3
import pandas as pd
import time
import logging
import random
import numpy as np
import os

dir_path = os.path.dirname(os.path.realpath(__file__))


def update_measurement_name(source: pd.DataFrame, measurment_name: str):
    count = 0
    for _ in source["iox::measurement"]:
        source.loc[count, "iox::measurement"] = measurment_name
        count += 1


def update_timestamps(source: pd.DataFrame):

    now = time.time_ns()
    interval = 333_000_000  # ms
    grit = random.randrange(0, 1_000_000)
    interval = interval + grit
    current = np.int64(now - interval * len(source["time"]))
    ts_type = type(source['time'][0]).__name__

    count = 0
    for _ in source['time']:
        if ts_type in ["int", "int32", "int64"]:
            source.loc[count, "time"] = current
        else:
            source.loc[count, "time"] = pd.Timestamp(current).__str__()
        count += 1
        current = current + interval

    if ts_type in ["int", "int32", "int64"]:
        source["time"] = source["time"].astype("int64")


def feather_update():
    logging.info("Updating feather")
    f_df = pd.read_feather(f"{dir_path}/out.feather")
    update_timestamps(f_df)
    update_measurement_name(f_df, "machine_data_feather")
    f_df.to_feather(f"{dir_path}/out_update.feather")


def orc_update():
    logging.info("Updating orc")
    o_df = pd.read_orc(f"{dir_path}/out.orc")
    update_timestamps(o_df)
    update_measurement_name(o_df, "machine_data_orc")
    o_df.to_orc(f"{dir_path}/out_update.orc", index=False)


def parquet_update():
    logging.info("Updating parquet")
    p_df = pd.read_parquet(f"{dir_path}/out.parquet")
    update_timestamps(p_df)
    update_measurement_name(p_df, "machine_data_parquet")
    p_df.to_parquet(f"{dir_path}/out_update.parquet", index=False)


def csv_update():
    logging.info("Updating csv")
    csv_df = pd.read_csv(f"{dir_path}/out.csv")
    update_timestamps(csv_df)
    csv_df.to_csv(f"{dir_path}/out_update.csv", index=False)


def json_update():
    logging.info("Updating json")
    json_df = pd.read_json(f"{dir_path}/out.json")
    update_timestamps(json_df)
    json_df.to_json(f"{dir_path}/out_update.json", orient='records', index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    feather_update()
    orc_update()
    parquet_update()
    csv_update()
    json_update()
