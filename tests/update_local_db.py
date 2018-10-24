#!/usr/bin/env python3

"""Execute a SQL query to udpate all dates and timestamps for Web API test purposes.

WARNING: use on a TEST db instance!

    > JITENSHEA_CONFIG=../config-test.ini ./update_local_db.py
"""


from subprocess import run, DEVNULL, PIPE, CalledProcessError

from jitenshea import config


cities = ["bordeaux", "lyon"]

psql_cmd = ["PGOPTIONS='--client-min-messages=warning'"]
configdb = config["database"]

if "password" in configdb and configdb.get("password", None):
    psql_cmd.append('PGPASSWORD="{}"'.format(configdb["password"]))

psql_cmd.append("psql")
psql_cmd.append("-v ON_ERROR_STOP=1")
psql_cmd.extend(
    [
        "{} {}".format(opt, configdb[cfg])
        for opt, cfg in (
            ("-h", "host"),
            ("-p", "port"),
            ("-d", "dbname"),
            ("-U", "user"),
        )
        if cfg in configdb and configdb.get(cfg, None)
    ]
)

# Query to shift all tables with timestamps and dates.
# It "updates" the data with recent dates. Usefull to test the Web API.
query = """

-- prediction. take the max from timeseries.
with ts as (
  select max(timestamp) as max_ts
    ,now() - max(timestamp) as diff
  from {city}.timeseries
  group by id
  limit 1
  )
update {city}.prediction
set timestamp = timestamp + diff
from ts;

-- timeseries
with ts as (
  select max(timestamp) as max_ts
    ,now() - max(timestamp) as diff
  from {city}.timeseries
  group by id
  limit 1
  )
update {city}.timeseries
set timestamp = timestamp + ts.diff
from ts;

-- daily transactions
with daily as (
  select max(date) as max_day
    ,now() - max(date) as diff
  from {city}.daily_transaction
  group by id
  limit 1
  )
update {city}.daily_transaction
set date = date + daily.diff
from daily;
"""


def execute_sql(query, city):
    """execute the query with psql
    """
    query = query.format(city=city)
    try:
        run(
            " ".join(psql_cmd),
            input=query,
            check=True,
            shell=True,
            stdout=DEVNULL,
            stderr=PIPE,
            encoding="UTF-8",
        )
    except CalledProcessError as exc:
        print("ERROR on", city, exc.stderr)
        raise exc


def main():
    for city in cities:
        print(f"processing {city}")
        execute_sql(query, city)


if __name__ == "__main__":
    main()
