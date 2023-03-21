import requests
import sys
import pandas as pd
from datetime import date
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

def github_get_traffic(username, repository_name, trafic_type = 'clones', headers={}):
    headers = headers

    url = f'https://api.github.com/repos/{username}/{repository_name}/traffic/{trafic_type}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    return data

def main():
    repos = [
        ('snowplow', 'dbt-snowplow-web'),
        ('snowplow', 'dbt-snowplow-mobile'),
        ('snowplow', 'dbt-snowplow-media-player'),
        ('snowplow', 'dbt-snowplow-utils'),
        ('snowplow', 'dbt-snowplow-fractribution'),
        ('snowplow', 'dbt-snowplow-ecommerce'),
        ('snowplow', 'dbt-snowplow-normalize'),
        ('snowplow', 'snowplow-tracking-cli'),
        ('snowplow', 'snowplow-dotnet-tracker'),
        ('snowplow', 'snowplow-golang-tracker'),
        ('snowplow', 'snowplow-java-tracker'),
        ('snowplow', 'snowplow-php-tracker'),
        ('snowplow', 'snowplow-python-tracker'),
        ('snowplow', 'snowplow-ruby-tracker'),
        ('snowplow', 'snowplow-scala-tracker'),
        ('snowplow', 'snowplow-unity-tracker'),
        ('snowplow', 'snowplow-cpp-tracker'),
        ('snowplow', 'snowplow-rust-tracker'),
        ('snowplow', 'snowplow-lua-tracker'),
        ('snowplow', 'snowplow-javascript-tracker'),
        ('snowplow', 'snowplow-android-tracker'),
        ('snowplow', 'snowplow-objc-tracker'),
        # ('snowplow-incubator', 'snowplow-react-native-tracker'),
        # ('snowplow-incubator', 'snowplow-roku-tracker'),
        # ('snowplow-incubator', 'snowplow-flutter-tracker'),
        ]

    #Set your PAT key so you get the 5000 calls per hour for the github api
    # call with `token <YOUR_TOKEN> as cmd argument if using a local PAT key`
    headers = {'Authorization': f"{sys.argv[1]}"}
    df = pd.DataFrame()
    for repo in repos:
        # Clones
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'clones', headers)['clones'])
        df_temp['metric'] = 'clones'
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
        # Popular paths
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'popular/paths', headers)).drop(columns=['title']).rename(columns={'path':'value'})
        df_temp['metric'] = 'popular/paths'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
        # Popular referrers
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'popular/referrers', headers)).rename(columns={'referrer':'value'})
        df_temp['metric'] = 'popular/referrers'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
        # Views
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'views', headers)['views'])
        df_temp['metric'] = 'views'
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])

    # Are there better ways to do this? Yes, of course there are.
    # Do I have more than a passing familiarity with pandas to know that way? No, I do not.
    df = df.convert_dtypes()
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    if len(df. index) > 0:
        con = snowflake.connector.connect(
            user = os.getenv('SNOWFLAKE_USER'),
            password = os.getenv('SNOWFLAKE_PASSWORD'),
            account = os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
            database = os.getenv('SNOWFLAKE_DATABASE'),
            schema = os.getenv('SNOWFLAKE_SCHEMA'),
            role = os.getenv('SNOWFLAKE_USER_ROLE'),
            session_parameters = {
                'QUERY_TAG': 'githubMetrics'
            }
        )

    # Write the data from the DataFrame to the table staging table
        success, _, nrows, _ = write_pandas(con, df, 'metrics_stg', auto_create_table=True, quote_identifiers=False)

        if success:
            print(f"Written {nrows} to `metrics_stg`")
        else:
            raise ValueError("Something went wrong")

        # Why am I connecting again? Great question, ask Snowflake why their package only does 2 things.
        engine = create_engine(URL(
            account = os.getenv('SNOWFLAKE_ACCOUNT'),
            user = os.getenv('SNOWFLAKE_USER'),
            password = os.getenv('SNOWFLAKE_PASSWORD'),
            database = os.getenv('SNOWFLAKE_DATABASE'),
            schema =  os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
            role = os.getenv('SNOWFLAKE_USER_ROLE'),
        ))
        connection = engine.connect()

        try:
            # there is a mergeInto function but it's more work than just writing the sql manually
            merge = "merge into metrics t using metrics_stg s on t.metric = s.metric and t.timestamp = s.timestamp and t.repo = s.repo when not matched then insert (timestamp, count, uniques, metric, value, rank, repo) values (s.timestamp, s.count, s.uniques, s.metric, s.value, s.rank, s.repo)"
            connection.execute(merge)
        finally:
            connection.close()
            engine.dispose()


if __name__ == '__main__':
    main()
