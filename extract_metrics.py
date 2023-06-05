import requests
import sys
import pandas as pd
from datetime import date, timedelta
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pypistats

def github_get_traffic(username, repository_name, trafic_type = 'clones', headers={}):
    headers = headers

    url = f'https://api.github.com/repos/{username}/{repository_name}/traffic/{trafic_type}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    return data

def get_repo_stats(repo, headers):
    df = pd.DataFrame()
    # Clones

    try: # incase of no results over the 14 day window
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'clones', headers)['clones'])
        df_temp['metric'] = 'clones'
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
    except:
        pass
    # Popular paths
    try:
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'popular/paths', headers)).drop(columns=['title']).rename(columns={'path':'value'})
        df_temp['metric'] = 'popular/paths'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
    except:
        pass
    # Popular referrers
    try:
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'popular/referrers', headers)).rename(columns={'referrer':'value'})
        df_temp['metric'] = 'popular/referrers'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
    except:
        pass
    # Views
    try:
        df_temp = pd.DataFrame.from_dict(github_get_traffic(repo[0], repo[1], 'views', headers)['views'])
        df_temp['metric'] = 'views'
        df_temp['repo'] = repo[1]
        df = pd.concat([df, df_temp])
    except:
        pass

    return df

def get_python_tracker(days = 3):
    python_installs = pypistats.overall("snowplow-tracker", format="pandas", total = True, start_date = str(date.today() - timedelta(days)), mirrors=True)
    python_installs['metric'] = 'installs'
    python_installs['repo'] = 'snowplow-python-tracker'
    python_installs['count'] = python_installs['downloads']
    python_installs['uniques'] = python_installs['downloads']
    python_installs['timestamp'] = python_installs['date']
    python_installs = python_installs[['timestamp', 'count', 'uniques', 'repo', 'metric']]
    python_installs = python_installs.iloc[:-1] # remove total row at the end

    return python_installs

def get_cratesio_stats(package= '', repo_name = 'snowplow-rust-tracker'):
    url = f'https://crates.io/api/v1/crates/{package}/downloads'
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    df_temp = pd.DataFrame.from_dict(data['version_downloads'])
    df_temp = df_temp.groupby(['date'], as_index=False)['downloads'].sum()
    df_temp['timestamp'] = df_temp['date']
    df_temp['repo'] = repo_name
    df_temp['metric'] = 'downloads'
    df_temp['count'] = df_temp['downloads']
    df_temp['uniques'] = df_temp['downloads']

    return df_temp[['timestamp', 'count', 'uniques', 'repo', 'metric']]

def get_npm_package_stats(package= '', days = 3):
    url = f'https://api.npmjs.org/downloads/range/{str(date.today() - timedelta(days))}:{str(date.today() - timedelta(1))}/{package}'
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    df_temp = pd.DataFrame.from_dict(data['downloads'])
    df_temp['timestamp'] = df_temp['day']
    df_temp['repo'] = package
    df_temp['metric'] = 'downloads'
    df_temp['count'] = df_temp['downloads']
    df_temp['uniques'] = df_temp['downloads']

    return df_temp[['timestamp', 'count', 'uniques', 'repo', 'metric']]

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
        ('snowplow-incubator', 'snowplow-react-native-tracker'),
        ('snowplow-incubator', 'snowplow-roku-tracker'),
        ('snowplow-incubator', 'snowplow-flutter-tracker'),
        ]
    npm_packages = ['@snowplow/react-native-tracker',
                    '@snowplow/browser-plugin-ad-tracking',
                    '@snowplow/browser-plugin-browser-features',
                    '@snowplow/browser-plugin-client-hints',
                    '@snowplow/browser-plugin-consent',
                    '@snowplow/browser-plugin-ecommerce',
                    '@snowplow/browser-plugin-enhanced-ecommerce',
                    '@snowplow/browser-plugin-error-tracking',
                    '@snowplow/browser-plugin-form-tracking',
                    '@snowplow/browser-plugin-ga-cookies',
                    '@snowplow/browser-plugin-geolocation',
                    '@snowplow/browser-plugin-link-click-tracking',
                    '@snowplow/browser-plugin-optimizely',
                    '@snowplow/browser-plugin-optimizely-x',
                    '@snowplow/browser-plugin-parrable',
                    '@snowplow/browser-plugin-performance-timing',
                    '@snowplow/browser-plugin-site-tracking',
                    '@snowplow/browser-plugin-timezone',
                    '@snowplow/browser-tracker',
                    '@snowplow/browser-tracker-core',
                    '@snowplow/javascript-tracker',
                    '@snowplow/tracker-core',
                    '@snowplow/browser-plugin-debugger',
                    '@snowplow/node-tracker',
                    '@snowplow/roku-tracker',
                    '@snowplow/browser-plugin-media-tracking',
                    '@snowplow/browser-plugin-youtube-tracking',
                    '@snowplow/webview-tracker',
                    '@snowplow/browser-plugin-enhanced-consent',
                    '@snowplow/browser-plugin-snowplow-ecommerce',
                    '@snowplow/browser-plugin-focalmeter']

    #Set your PAT key so you get the 5000 calls per hour for the github api
    # call with `token <YOUR_TOKEN> as cmd argument if using a local PAT key`
    headers = {'Authorization': f"{sys.argv[1]}"}
    all_df = pd.DataFrame()

    # Github traffic
    for repo in repos:
        print(f'Getting repo {repo[1]}')
        df_temp = get_repo_stats(repo, headers)
        all_df = pd.concat([all_df, df_temp])

    # Python installs
    all_df = pd.concat([all_df, get_python_tracker(3)])

    # crate.io fails on GH actions for some reason, I assume rate limiting.
    # all_df = pd.concat([all_df, get_cratesio_stats('snowplow_tracker', 'snowplow-rust-tracker')])


    # npm packages
    for package in npm_packages:
        print(f'Getting package {package}')
        df_temp = get_npm_package_stats(package, 3)
        all_df = pd.concat([all_df, df_temp])

    # Are there better ways to do this? Yes, of course there are.
    # Do I have more than a passing familiarity with pandas to know that way? No, I do not.
    all_df = all_df.convert_dtypes()
    all_df['timestamp'] = pd.to_datetime(all_df['timestamp'])

    if len(all_df. index) > 0:
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
        success, _, nrows, _ = write_pandas(con, all_df, 'metrics_stg', auto_create_table=True, quote_identifiers=False, overwrite=True)

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
            merge = """merge into metrics t using metrics_stg s
                            on t.metric = s.metric
                                and t.timestamp = s.timestamp
                                and t.repo = s.repo
                        when matched and t.metric in ('views', 'clones', 'downloads', 'installs') then update set t.uniques = s.uniques, t.count = s.count
                        when not matched then insert (timestamp, count, uniques, metric, value, rank, repo) values (s.timestamp, s.count, s.uniques, s.metric, s.value, s.rank, s.repo)
                """
            connection.execute(merge)
        finally:
            connection.close()
            engine.dispose()


if __name__ == '__main__':
    main()
