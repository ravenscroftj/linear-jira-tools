import click
import requests
import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()



from markup_tools import run_pandoc

def export_projects():

    q = {"query": """
    query{
        projects {
            edges{
                node {
                    id
                    name
                    description
                    startDate
                    targetDate
                    startedAt
                    completedAt
                    canceledAt
                    state
                }
                cursor
            }
            pageInfo {
                hasNextPage
                endCursor
            }
        }

    }
    """}



    r = requests.post("https://api.linear.app/graphql", json=q, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})

    data = r.json()
    projects = []
    while data['data']['projects']['pageInfo']['hasNextPage']:
        projects.extend(data['data']['projects']['edges'])
        qnext = {"query": """
            query Projects($cursor: String) {
                projects (first: 50, after: $cursor) {
                    edges{
                        node {
                            id
                            name
                            description
                            startDate
                            targetDate
                            startedAt
                            completedAt
                            canceledAt
                            state
                        }
                        cursor
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }

            }
            """, "variables": {"cursor": data['data']['projects']['pageInfo']['endCursor'] }}

        data = requests.post("https://api.linear.app/graphql", json=qnext, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"}).json()

        print(data)

        print(data['data']['projects']['edges'][0]['node']['name'])

    projects.extend(data['data']['projects']['edges'])

    projects_df = pd.DataFrame.from_records([ x['node'] for x in projects ])

    return projects_df
    
@click.command()
@click.argument('output_file', type=click.Path(file_okay=True))
def main(output_file):
    
    projects_df = export_projects()

    projects_df['ID'] = "PROJECT-" + projects_df.reset_index()['index'].astype(str)

    date_fields = ['startDate','targetDate','startedAt','completedAt','canceledAt']

    for field in date_fields:
        projects_df[field] = pd.to_datetime(projects_df[field])

    projects_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()