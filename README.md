# About
This is a community repository of Python code for InfluxDB with IOx. While this code is built on officially supported APIs, the library and CLI here are not officially support by Influx Data. 

When installed, you have access to 2 pieces of functionality:
1. A CLI for reading and writing data to InfluxDB with IOx.
2. A client library for reading and writing data to InfluxDB with IOx.

# Install
To install only the client:
```bash
python3 -m pip install pyinflux3
```
To install the client and CLI:
```bash
sudo python3 -m pip install "pyinflux3[cli]"
```
***Note: Use sudo if you would like to directly install the client onto your path. Otherwise use the `--user` flag.**

# Add a Config
You can drop a config file called config.json in the directory where you are running the influx3 command:

```json
{
    "my-config": {
        "database": "your-database",
        "host": "your-host",
        "token": "your-token",
        "org": "your-org-id",
        "active": true
    }
}
```

Or you can use the config command to create or modify a config:
```
% influx3 config --name="my-config" --database="<database or bucket name>" --host="us-east-1-1.aws.cloud2.influxdata.com" --token="<your token>" --org="<your org ID>"
```

If you are running against InfluxDB Cloud Serverless, then use the bucket name for the database in you configuration.

# Run as a Command
```
% influx3 sql "select * from anomalies"
```

```
% influx3 write testmes f=7 
```

# Query and Write Interactively


```
% influx3
Welcome to my IOx CLI.

(>) sql
(sql >) select * from anomalies
    check    id  observed                          time     type user_id  value
0       1  None       NaN 2023-02-03 20:56:57.513279776    error       1  400.0
1       1  None       NaN 2023-02-03 17:52:54.328785835  latency       1  900.0
```

```
(>) write 
testmes f=5 boring-observability
```

# Write from a File
Both the InfluxDB CLI and Client libary support writing from a CSV file. The CSV file must have a header row with the column names. The there must be a column containing a timestamp. Here are the parse options:
* `--file` - The path to the csv file.
* `--time` - The name of the column containing the timestamp.
* `--measurment` - The name of the measurment to store the CSV data under. (Currently only supports user specified string)
* `--tags` - (optional) Specify an array of column names to use as tags. (Currently only supports user specified strings) for example: `--tags=host,region`

```bash
influx3 write_csv --file ./Examples/example.csv --measurement table2 --time Date --tags host,region
```


# Client library
This project also includes a new client library that strives for utter simplicity. It includes 3 functions, a constuctor, write(), and read().

# Contribution
If you are working on a new feature for either the CLI or the Client Libary please make sure you test both for breaking changes. This can currently be achived using the following method:
```bash
python3 -m venv .venv
source .venv/bin/activate
chmod +x ./test/test-package.sh 
./test/test-package.sh 
```
Any time you make changes in your code and want to retest just run the script again:
```
./test/test-package.sh 
```

