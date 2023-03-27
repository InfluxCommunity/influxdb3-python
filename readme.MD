# About
This tool is an interactive shell for working with IOx-based InfluxDB instances. Currently, only querying is supported. 

# Add a Config
You need to drop a config files call config.json next to the python code to make it work for now. 

```json
{
    "host":"",
    "token":"",
    "bucket":""
}
```

# Run and Query
So far only the query command is supported.

```bash
% ./inflightsql.py
Welcome to my IOx CLI.

(>) query
(query >) select * from anomalies
    check    id  observed                          time     type user_id  value
0       1  None       NaN 2023-02-03 20:56:57.513279776    error       1  400.0
1       1  None       NaN 2023-02-03 17:52:54.328785835  latency       1  900.0
```