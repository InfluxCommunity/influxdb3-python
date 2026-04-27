#!/usr/bin/env python3
"""
downsample.py - is a functional example showing how to reduce a larger measurement set to a smaller one
by averaging values across a given time window.
"""
import datetime
import os
import random

import pandas as pd

from influxdb_client_3 import InfluxDBClient3, InfluxDBError, WriteOptions, write_client_options

HOST = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
TOKEN = os.getenv('INFLUXDB_TOKEN') or 'my-token'
DATABASE = os.getenv('INFLUXDB_DATABASE') or 'my-db'


class BatchingCallback(object):

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


callback = BatchingCallback()

write_options = WriteOptions(batch_size=100,
                             flush_interval=10_000,
                             jitter_interval=2_000,
                             retry_interval=5_000,
                             max_retries=5,
                             max_retry_delay=30_000,
                             exponential_base=2)

wco = write_client_options(success_callback=callback.success,
                           error_callback=callback.error,
                           retry_callback=callback.retry,
                           write_options=write_options
                           )

now = pd.Timestamp.now(tz='UTC').floor('ms')

current = now - datetime.timedelta(days=1)

# Lists of possible trainers
trainers = ["ash", "brock", "misty", "gary", "jessie", "james"]

# Read the CSV into a DataFrame
pokemon_df = pd.read_csv(
    "../write/source_data/pokemon.csv"
)

# Creating an empty list to store the original data
data = []

# Dictionary to keep track of the number of times each trainer has caught each Pokémon
trainer_pokemon_counts = {}

# Number of entries we want to create
num_entries = 1000

# use a first client to write the prep data
with InfluxDBClient3(
        token=TOKEN,
        host=HOST,
        database=DATABASE,
        enable_gzip=True,
        write_client_options=wco) as prep_client:

    # Generating random data
    for i in range(num_entries):
        trainer = random.choice(trainers)

        # Randomly select a row from pokemon_df
        random_pokemon = pokemon_df.sample().iloc[0]
        caught = random_pokemon['Name']

        # Count the number of times this trainer has caught this Pokémon
        if (trainer, caught) in trainer_pokemon_counts:
            trainer_pokemon_counts[(trainer, caught)] += 1
        else:
            trainer_pokemon_counts[(trainer, caught)] = 1

        # Get the number for this combination of trainer and Pokémon
        num = trainer_pokemon_counts[(trainer, caught)]

        entry = {
            "trainer": trainer,
            "id": f"{0000 + random_pokemon['#']:04d}",
            "num": num,
            "caught": caught,
            "level": random.randint(5, 20),
            "attack": random_pokemon['Attack'],
            "defense": random_pokemon['Defense'],
            "hp": random_pokemon['HP'],
            "speed": random_pokemon['Speed'],
            "type1": random_pokemon['Type 1'],
            "type2": random_pokemon['Type 2'],
            "legendary": random_pokemon['Legendary'],
            "timestamp": current
        }
        data.append(entry)
        current = current + datetime.timedelta(seconds=int((24 * 60 * 60) / num_entries))

    # Convert the list of dictionaries to a DataFrame
    caught_pokemon_df = pd.DataFrame(data).set_index('timestamp')

    # Print the DataFrame
    print(caught_pokemon_df)

    # Write the data directly from the DataFrame
    # taking care to specify the measurement name and tags
    try:
        prep_client.write(caught_pokemon_df, data_frame_measurement_name='monsters_caught',
                          data_frame_tag_columns=['trainer', 'id', 'type1', 'type2', "legendary", "caught"])
    except Exception as e:
        print(f"Error writing point: {e}")

# Now query just written data and downsample
with InfluxDBClient3(
        token=TOKEN,
        host=HOST,
        database=DATABASE, enable_gzip=True, write_client_options=wco) as ds_client:

    # downsample data to average number of catches per quarter-hour
    sql = ("SELECT date_bin('15 minutes', \"time\") as window_start, \n"
           "AVG(\"num\") as avg\n"
           "FROM monsters_caught\n"
           "WHERE \"time\" >= now() - interval '1 day'\n"
           "GROUP BY window_start\n"
           "ORDER BY window_start ASC"
           )

    # Query directly to a pandas DataFrame
    interim_df = ds_client.query(sql, language="sql", mode="pandas")

    # Modify the DataFrame to make the columns Influx friendly
    interim_df.rename(columns={'window_start': 'timestamp'}, inplace=True)
    interim_df["context"] = pd.Series('demo', index=interim_df.index)

    interim_df.set_index('timestamp', inplace=True)

    # Write the downsampled DataFrame to a new measurement in the database
    try:
        ds_client.write(interim_df, data_frame_measurement_name='caught_avg',
                        data_frame_tag_columns=['context'])
    except Exception as e:
        print(f"Error writing down sampled point: {e}")
