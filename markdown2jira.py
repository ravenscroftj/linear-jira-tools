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