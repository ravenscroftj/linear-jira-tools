import os
import click
import subprocess
import numpy as np
from tqdm.auto import tqdm
import pandas as pd

import mistletoe
import pandas as pd

from mistletoe.contrib import jira_renderer

tqdm.pandas()
# # Function to run shell command on each description

# #LUA_PATH = os.path.join(os.path.dirname(__file__), "jira.lua")

# def run_pandoc(description):

#     try:

#         if pd.isna(description):
#             return description


#         # Running the pandoc command with the description as input

#         result = subprocess.run(['pandoc', '--to', LUA_PATH, '-o', '-'], 
#                                 input=description, text=True, capture_output=True)

#         # Return the stdout if the command was successful

#         if result.returncode == 0:

#             return result.stdout

#         else:

#             return f"Error: {result.stderr}"


#     except Exception as e:

#         return str(e)


def render_jira(body):
    return mistletoe.markdown(body, jira_renderer.JIRARenderer)



@click.command()
@click.option("--input-file", '-i', type=click.Path(file_okay=True), required=True)
@click.option('--output-file','-o', type=click.Path(file_okay=True), required=True)
@click.option('--target-column', '-c', type=str)
def main(input_file, output_file, target_column):

    tqdm.pandas()
    df = pd.read_csv(input_file)

    if target_column not in df:
        raise Exception(f'The column {target_column} does not exist in the csv {input_file}')
    
    df[target_column] = df[target_column].progress_apply(render_jira)

    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()