import os
import click
import json
import pandas as pd
import numpy as np
import yaml

from functools import partial


def email_to_user_record(email):
    user,domain = email.split('@')

    return {'name':user,'email':email}


def map_custom_fields(row):

    fields = []

    if pd.notna(row['Estimate']):
        fields.append({
            'fieldName': 'Linear Estimate',
            'fieldType':'com.atlassian.jira.plugin.system.customfieldtypes:float',
            'value': str(row['Estimate'])
        })

    return fields




def map_labels(row):
    labels = []

    if pd.notna(row['Labels']):
        labels = row['Labels'].split(',')

    if pd.notna(row['Team']):
        labels.append(row['Team'])


    if row.Status in ['Delivery Backlog', 'To Do', 'Todo']:
        labels.append('Delivery_Backlog_Dumpster')

    if row.Status in ['Opportunity Backlog', 'Discovery', 'Prototyping & Design']:
        labels.append('Discovery_Backlog_Dumpster')

    labels = [x.strip().replace(' ','_') for x in labels]
    return labels

def map_links(row):

    relationship_name = "sub-task-link"

    if 'Bug' in row['Labels']:
        relationship_name = 'Relates'

    if pd.isna(row['Parent issue']):

        if pd.isna(row['Project ID']):
            return np.nan
        else:
            return { 'name': relationship_name,  
                    'destinationId': str(row['Project ID']),
                    'sourceId': str(row['ID'])
            }
    else:
            return { 'name': relationship_name,  
                    'destinationId': str(row['Parent issue']),
                    'sourceId': str(row['ID'])
            }
    
def map_issue_type(row):

    if pd.notna(row['Labels']) and ('Bug' in row['Labels']):
        return 'Bug'

    elif pd.notna(row['Parent issue']):
        return 'Sub-Task'
    else:
        return 'Story'
    
def set_resolved_datestamp(row, colNames=['canceledAt','completedAt']):
    """In Jira resolved applies to canceled or completed."""

    # assuming that only one of these columns will have a value - if multiple do then 
    # we will arbitrarily pick the first one that we check in the colNames list

    for col in colNames:
        if pd.notna(row[col]):
            return row[col]
    
    return pd.NA

def map_status(row, status_col, issue_type_col, mappings):

    specific = mappings.get(row[issue_type_col],{}).get(row[status_col])

    if specific is not None:
        return specific
    
    default_mapping = mappings.get('default',{}).get(row[status_col])


    if default_mapping is not None:
        return default_mapping

    return row[status_col]
    


@click.command()
@click.option('--config-file', required=True)
@click.option('--limit-to-issue-ids', required=False, default=None, help="If set then limit generation to the given issue ids")
@click.option('--projects-file',required=True)
@click.option('--issues-file',required=True)
@click.option('--output-file',required=True)
@click.option('--comments-file', required=False)
@click.option('--attachments-file', required=False)
def main(issues_file, output_file, config_file, projects_file, comments_file=None, attachments_file=None, limit_to_issue_ids=None):

    # https://confluence.atlassian.com/adminjiraserver/importing-data-from-json-938847609.html
    json_doc = {
        'users':[],
        'links':[],
        'projects':[

        ]
    }

    with open(config_file) as f:
        config = yaml.safe_load(f)

    project = config['jira_project_template']

    linear_export_teams = config.get('export_teams')


    issues_df = pd.read_csv(issues_file)

    # filter teams
    if (linear_export_teams is not None) and (len(linear_export_teams) > 0):
        issues_df = issues_df[issues_df.Team.isin(linear_export_teams)]


    creators =  [email_to_user_record(x) for x in issues_df['Creator'].unique() if pd.notna(x) ]
    assignees = [email_to_user_record(x) for x in issues_df['Assignee'].unique() if pd.notna(x) ]
    json_doc['users'] = creators + assignees
    user_map = { x['email']:x['name'] for x in json_doc['users'] }

    # if a specific set of issue ids is given, truncate the list now
    if limit_to_issue_ids:
        id_list = [x.strip() for x in limit_to_issue_ids.split(',')]
        issues_df = issues_df[issues_df.ID.isin(id_list)]

    projects_df = pd.read_csv(projects_file)

    if limit_to_issue_ids:
        id_list = [x.strip() for x in limit_to_issue_ids.split(',')]
        projects_df = projects_df[projects_df.id.isin(id_list)]

    map_epic_status = lambda row: map_status(row, 'state', 'issueType', config['status_map'])

    projects_df['issueType'] = 'Epic'
    projects_df['status'] = projects_df.apply(map_epic_status, axis=1)
    projects_df['description'] = projects_df.description.fillna('')
    
    date_format = "%Y-%m-%dT%H:%M:%S.%f%z"

    for timefield in ['startDate','targetDate','completedAt','canceledAt']:
        projects_df[timefield] = pd.to_datetime(projects_df[timefield])
        projects_df[timefield] = projects_df[timefield].apply(lambda x: x.strftime(date_format) if pd.notna(x) else x)

    
    projects_df['resolved'] = projects_df.apply(lambda x: set_resolved_datestamp(x, ['canceledAt','completedAt']), axis=1)

    for col in ['startDate','targetDate','resolved']:
        projects_df[col].fillna('', inplace=True)


    projects_df['startedAt'].fillna('',inplace=True)

    project_df_map={'name':'summary', 'startedAt':'started', 'id':'externalId' ,'targetDate':'duedate'}
    project_df_cols = ['status','description','summary','issueType','started', 'externalId', 'resolved','duedate']

    epics = projects_df.rename(columns=project_df_map)[project_df_cols].to_dict(orient='records')

    issues_df_map = {'ID':'externalId','Title':'summary','Description':'description', 'Labels':'labels', 'Created':'created','Started':'started', 'Priority':'priority'}
    issues_df_cols = ['externalId','summary','description','reporter', 'customFieldValues','labels', 'issueType', 'created','started','resolved', 'status', 'priority', 'assignee']

    # we have to parse the crazy linear timestamp format into something sensible
    for col in ['Completed','Canceled','Started','Created']:
        issues_df[col] = issues_df[col].str.replace(r' GMT\+0000 \(GMT\)', '', regex=True)
        issues_df[col] = pd.to_datetime(issues_df[col], format='%a %b %d %Y %H:%M:%S')
        issues_df[col] = issues_df[col].apply(lambda x: x.strftime(date_format) if pd.notna(x) else x)

    issues_df['resolved'] = issues_df.apply(lambda x: set_resolved_datestamp(x, ['Canceled','Completed']), axis=1)

    for col in ['Started','Created','resolved']:
        issues_df[col].fillna('', inplace=True)
    
    map_issue_status = lambda row: map_status(row, 'Status', 'issueType', config['status_map'])


    issues_df['issueType'] = issues_df.apply(map_issue_type, axis=1)
    issues_df['status'] = issues_df.apply(map_issue_status, axis=1)

    issues_df['Description'].fillna('',inplace=True)
    query = issues_df['Parent issue'].isna() & issues_df['Project ID'].notna()
    issues_df.loc[query, 'Parent issue'] = issues_df[query]['Project ID']

    issues_df['reporter'] = issues_df['Creator'].apply(lambda x: user_map[x] if pd.notna(x) else '')
    issues_df['assignee'] = issues_df['Assignee'].apply(lambda x: user_map[x] if pd.notna(x) else '')
    issues_df['customFieldValues'] = issues_df.apply(map_custom_fields, axis=1)


    subtasks = issues_df[issues_df['issueType'] == 'Sub-Task']['ID'].values
    issues_df[issues_df['Parent issue'].isin(subtasks)] 
    tasks = set(issues_df[issues_df['Parent issue'].isin(subtasks)]['Parent issue'].values)
    issues_df.loc[issues_df['ID'].isin(tasks), 'issueType'] = 'Task'

    issues_df['Labels'] = issues_df.apply(map_labels, axis=1)

    issues = issues_df.rename(columns=issues_df_map)[issues_df_cols].to_dict(orient='records')

    if comments_file is not None:

        comments_df = pd.read_csv(comments_file)

        # extend the user map to comments as well
        json_doc['users'] = json_doc['users'] + [email_to_user_record(x) for x in comments_df['node.user.email'].unique() if pd.notna(x) ]
        user_map = { x['email']:x['name'] for x in json_doc['users'] }


        comments_df['created'] = pd.to_datetime(comments_df['node.createdAt'])

        # map comment author to new username
        comments_df['author'] = comments_df['node.user.email'].apply(lambda x: user_map[x] if pd.notna(x) else pd.NA)

        comment_columns_map = {
            'node.body':'body'
        }

        comment_columns = ['body','author','created']

        for issue in issues:
            
            comments = pd.DataFrame(comments_df[comments_df['node.issue.identifier'] == issue['externalId']].sort_values(by='created'))

            comments.drop_duplicates(subset=['node.id'], inplace=True)

            if len(comments) > 0:
                comments['created'] = comments['created'].apply(lambda x: x.strftime(date_format) if pd.notna(x) else x).fillna('')
                issue['comments'] = comments.rename(columns=comment_columns_map)[comment_columns].to_dict(orient='records')

    if attachments_file is not None:

        attachments_df = pd.read_csv(attachments_file)

        for issue in issues:
            attachments = attachments_df[attachments_df['issueId'] == issue['externalId']]

            if len(attachments) > 0:
                issue['attachments'] = attachments.rename(columns={'filenames':'name'})[['name','uri']].to_dict(orient='records')

            

    links = issues_df.apply(map_links, axis=1)

    json_doc['links'] = links[links.notna()].tolist()


    project['issues'] = epics + issues
    

    json_doc['projects'].append(project)

    with open(output_file,'w') as f:
        json.dump(json_doc, f, indent=2)
    


if __name__ == "__main__":
    main()