import os
import click
import subprocess
import numpy as np
from tqdm.auto import tqdm
import pandas as pd
import functools

tqdm.pandas()

def clean_labels(row, map_team=False):

    label = row['Labels'] if pd.notna(row['Labels']) else ""

    labels = label.split(',')
    
    labels = [ x.replace(' ','_') for x in labels ]

    if map_team:
        # also append team name
        labels.append(row['Team'].replace(' ','_'))

    return " ".join(labels)

@click.command()
@click.option("--projects-file", '-p', type=click.Path(file_okay=True), required=True)
@click.option("--issues-file", '-i', type=click.Path(file_okay=True), required=True)
@click.option('--output-file','-o', type=click.Path(file_okay=True), required=True)
@click.option('--label-teams/--no-label-teams', default=False, is_flag=True)
def main(projects_file, issues_file, output_file, label_teams):

    tqdm.pandas()
    df = pd.read_csv(issues_file)
    projects_df = pd.read_csv(projects_file).rename(columns={
        'id':'ID',
        'name':'Title',
        'description':'Description',
        'startDate':'Started',
        'targetDate':'Due Date',
        'completedAt':'Completed'
    })

    # do the magic conversion of datestamps on the issues file

    for col in ['Completed','Canceled','Started','Created']:
        df[col] = df[col].str.replace(r' GMT\+0000 \(GMT\)', '', regex=True)
        df[col] = pd.to_datetime(df[col], format='%a %b %d %Y %H:%M:%S')


    # re-map issues that don't have a parent issue but do have a project ID
    query = df['Parent issue'].isna() & df['Project ID'].notna()
    df.loc[query, 'Parent issue'] = df[query]['Project ID']

    # all projects map to 'Epic' in the new world.
    projects_df['Issue Type'] = 'Epic'

    full_output = pd.concat([ projects_df[['ID','Title','Description','Issue Type', 'Started', 'Due Date','Completed']], df])

    full_output['Labels'] = full_output.Labels.apply(functools.partial(clean_labels, map_team=label_teams))

    full_output.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()