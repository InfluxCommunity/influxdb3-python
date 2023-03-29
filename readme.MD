# About
This tool is an interactive shell for working with IOx-based InfluxDB instances. Supports writing data and quering with SQL.

I have cheekily named it "influx3" for now.

# Add a Config
You can drop a config files call config.json next to the python code: 

```json
{
{
    "my-config": {
        "namespace": "your-namespace",
        "host": "your-host",
        "token": "your-token",
        "org": "your-org-id",
        "active": true
    }
}
}
```

Or you can use the config command to create or modify a config:
```
% ./influx3 config --name="my-config" --namespace="boring-observability" --host="us-east-1-1.aws.cloud2.influxdata.com" --token="bBBvtg5EBsa9iayvmP36UtN327gQti1D-1uReiptl_gEfODHmGFxU2IgFdoAWgJxltl8qanrSU4Q3a8nUIrHsQ==" --org="847e9dbb25976492"
```

If you are running against InfluxDB Cloud, then use the bucket name for the namespace in you configuration.

# Run as a Command
```
% ./influx3 sql "select * from anomalies"
```

```
% ./influx3 write testmes f=7 
```

# Run and Query Interactively
So far only the query command is supported.

```
% ./influx3
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