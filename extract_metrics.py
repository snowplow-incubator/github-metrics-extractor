import requests
import sys
import pandas as pd
from datetime import date
import pickle
from snowplow_tracker import Snowplow, EmitterConfiguration, Tracker, Emitter, TrackerConfiguration, SelfDescribingJson

def github_get_traffic(username, repository_name, trafic_type = 'clones', headers={}):
    headers = headers

    url = f'https://api.github.com/repos/{username}/{repository_name}/traffic/{trafic_type}'
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    return data


def main():
    repos = [
        'dbt-snowplow-web',
        'dbt-snowplow-mobile',
        'dbt-snowplow-media-player',
        'dbt-snowplow-utils',
        'dbt-snowplow-fractribution',
        'dbt-snowplow-ecommerce',
        'dbt-snowplow-normalize',
        ]

    #Set your PAT key so you get the 5000 calls per hour for the github api
    # call with `token <YOUR_TOKEN> as cmd argument if using a local PAT key`
    headers = {'Authorization': f"token {sys.argv[1]}"}
    username = 'snowplow'
    df = pd.DataFrame()
    for repo in repos:
        # Clones
        df_temp = pd.DataFrame.from_dict(github_get_traffic(username, repo, 'clones', headers)['clones'])
        df_temp['metric'] = 'clones'
        df = pd.concat([df, df_temp])
        # Popular paths
        df_temp = pd.DataFrame.from_dict(github_get_traffic(username, repo, 'popular/paths', headers)).drop(columns=['title']).rename(columns={'path':'value'})
        df_temp['metric'] = 'popular/paths'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df = pd.concat([df, df_temp])
        # Popular referrers
        df_temp = pd.DataFrame.from_dict(github_get_traffic(username, repo, 'popular/referrers', headers)).rename(columns={'referrer':'value'})
        df_temp['metric'] = 'popular/referrers'
        df_temp['timestamp'] = date.today().strftime("%Y-%m-%dT%H:%M:%SZ")
        df_temp['rank'] = range(1, len(df_temp.index) + 1)
        df = pd.concat([df, df_temp])
        # Views
        df_temp = pd.DataFrame.from_dict(github_get_traffic(username, repo, 'views', headers)['views'])
        df_temp['metric'] = 'views'
        df = pd.concat([df, df_temp])

        df['repo'] = repo

    # Get previous max date
    try: # first run protection
        m_tstamp = pickle.load(open('./data/last_max.pkl', 'rb'))
    except:
        m_tstamp = pd.to_datetime('2020-01-01T00:00:00Z')

    # Are there better ways to do this? Yes, of course there are.
    # Do I have more than a passing familiarity with pandas to know that way? No, I do not.
    df = df.convert_dtypes()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['timestamp'] > m_tstamp]
    df = df.replace({pd.NA: None})

    # For now just store it
    if len(df. index) > 0:
        # df.to_csv(f'data/exported_metrics_{date.today()}.csv', index = False)

        e = Emitter("localhost:9090", protocol='http')

        t = Tracker(e,
                        app_id="gh_metrics",)
        for _, row in df.iterrows():
            print(f"sending event: {row['metric']}")
            t.track_self_describing_event(SelfDescribingJson(
            "iglu:com.snowplowanalytics/github_metrics/jsonschema/1-0-0",
            {
                "rank": row['rank'],
                "repo": row['repo'],
                "value": row['value'],
                "metric_tstamp": row['timestamp'].strftime("%Y-%m-%dT%H:%M:%S.0Z"),
                "uniques": row['uniques'],
                "metric": row['metric'],
                "count": row['count']
            }
            ))

        t.flush()

        # Write value so it's saved
        pickle.dump(max(df['timestamp']), open('./data/last_max.pkl', 'wb'))


if __name__ == '__main__':
    main()
