import click
import requests
import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


data_structure = """
    edges{
        node {
            id
            body
            user { email } 
            createdAt
            issue {
                id
                identifier
            }

            parent { id }
        }
        cursor
    }
    pageInfo {
        hasNextPage
        endCursor
    }
"""


def export_comments():

    q = {"query": f"""
    query {{
            comments {{
                {data_structure}
            }}

        }}
    """}

    r = requests.post("https://api.linear.app/graphql", json=q, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})

    data = r.json()
    comments = []
    print(data)
    while data['data']['comments']['pageInfo']['hasNextPage']:
        comments.extend(data['data']['comments']['edges'])

        qnext = {"query": f"""
                query Issues($cursor: String) {{
                    comments (first: 50, after: $cursor) {{
                        {data_structure}
                    }}
                }}
                """, 
                "variables": {"cursor": data['data']['comments']['pageInfo']['endCursor'] }
                }

        data = requests.post("https://api.linear.app/graphql", json=qnext, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"}).json()

        print(data)

        # print(data)

        # print(data['data']['comments']['edges'][0]['node']['name'])

        comments.extend(data['data']['comments']['edges'])

    #comments_df = pd.DataFrame.pd.json_normalize(data['data']['comments']['edges'])([ x['node'] for x in comments ])
    comments_df = pd.json_normalize(comments)

    return comments_df
    
@click.command()
@click.argument('output_file', type=click.Path(file_okay=True))
def main(output_file):
    
    comments_df = export_comments()

    # date_fields = ['startDate','targetDate','startedAt','completedAt','canceledAt']

    # for field in date_fields:
    #     comments_df[field] = pd.to_datetime(comments_df[field])

    comments_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()