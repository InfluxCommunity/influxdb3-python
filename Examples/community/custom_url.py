import random

import pandas as pd

from influxdb_client_3 import InfluxDBClient3, InfluxDBError, WriteOptions, write_client_options


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
                           WriteOptions=write_options
                           )

client = InfluxDBClient3(
    token="",
    host="https://eu-central-1-1.aws.cloud2.influxdata.com:442",
    org="6a841c0c08328fb1",
    database="pokemon-codex", enable_gzip=True, write_client_options=wco, write_port_overwrite=443,
    query_port_overwrite=443)

now = pd.Timestamp.now(tz='UTC').floor('ms')

# Lists of possible trainers
trainers = ["ash", "brock", "misty", "gary", "jessie", "james"]

# Read the CSV into a DataFrame
pokemon_df = pd.read_csv(
    "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv")  # noqa: E501

# Creating an empty list to store the data
data = []

# Dictionary to keep track of the number of times each trainer has caught each Pokémon
trainer_pokemon_counts = {}

# Number of entries we want to create
num_entries = 1000

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
        "num": str(num),
        "caught": caught,
        "level": random.randint(5, 20),
        "attack": random_pokemon['Attack'],
        "defense": random_pokemon['Defense'],
        "hp": random_pokemon['HP'],
        "speed": random_pokemon['Speed'],
        "type1": random_pokemon['Type 1'],
        "type2": random_pokemon['Type 2'],
        "timestamp": now
    }
    data.append(entry)

# Convert the list of dictionaries to a DataFrame
caught_pokemon_df = pd.DataFrame(data).set_index('timestamp')

# Print the DataFrame
print(caught_pokemon_df)

try:
    client.write(caught_pokemon_df, data_frame_measurement_name='caught',
                 data_frame_tag_columns=['trainer', 'id', 'num'])
except Exception as e:
    print(f"Error writing point: {e}")
