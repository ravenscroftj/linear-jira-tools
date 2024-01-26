import mistletoe
import click
import os
import pandas as pd
import requests
import dotenv
import tqdm
import uuid
import csv
from mistletoe.span_token import Image

def _extract_image_links(node):

    if isinstance(node, Image):
        yield (node.src)

    if hasattr(node, 'children'):
        for child in node.children:
            for img in _extract_image_links(child):
                yield img

def extract_image_links(description):
    doc = mistletoe.Document(description)
    return list(_extract_image_links(doc))
from tqdm.auto import tqdm

import cgi
import mimetypes


def guess_filename_for_row(row):

    filename = None
    r = None
    

    if row.get('filenames') is not None:
        filename = row.get('filenames')

    if filename is None:
        r = requests.head(row.imageUrl, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})

        if 'Content-Disposition' in r.headers:
            # Parse the Content-Disposition header
            _, params = cgi.parse_header(r.headers['Content-Disposition'])
            # Extract the filename
            filename = params.get('filename')

    
    if filename is not None:
        fn, ext = os.path.splitext(filename)

        if ext in [None,'']:
            filename=None

    if filename is None:
        if r is None:
            r = requests.head(row.imageUrl, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})
            
        filename = row['savedFilename'] + mimetypes.guess_extension(r.headers['content-type'].split(';')[0], strict=False)

    return filename


@click.command()
@click.option('--issues-file', required=True, type=click.Path(file_okay=True, dir_okay=False, exists=True))
@click.option('--output-dir', required=True, type=click.Path(dir_okay=True, file_okay=False))
def main(issues_file, output_dir):
    
    dotenv.load_dotenv()
    tqdm.pandas()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df = pd.read_csv(issues_file)

    df['image_urls'] = df.Description.fillna('').progress_apply(extract_image_links)

    mini_df = df[['ID','image_urls']].explode('image_urls').dropna(subset='image_urls')

    # although it would be more efficient to use progress_apply() we use iterrows so that we can
    # insert a 1s wait inbetween gets so that linear don't get angry with us
    filenames = []

    with open(os.path.join(output_dir, 'index.csv'), 'w') as index_f:

        csvw = csv.DictWriter(index_f, fieldnames=['issueId', 'imageUrl', 'oldFilename', 'savedFilename'])

        csvw.writeheader()
        
        for _, row in tqdm(mini_df.iterrows(), total=len(mini_df)):


            r = requests.head(row.image_urls, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})

            oldFilename = None

            if 'Content-Disposition' in r.headers:
                # Parse the Content-Disposition header
                _, params = cgi.parse_header(r.headers['Content-Disposition'])
                # Extract the filename
                oldFilename = params.get('filename')


            filename = uuid.uuid4().hex + mimetypes.guess_extension(r.headers['content-type'].split(';')[0], strict=False)

            r = requests.get(row.image_urls, headers={f"Authorization": f"{os.getenv('LINEAR_API_KEY')}"})


            with open(os.path.join(output_dir, filename), 'wb') as f:
                f.write(r.content)

            csvw.writerow({
                "issueId": row['ID'],
                'imageUrl': row['image_urls'],
                'oldFilename': oldFilename,
                'savedFilename': filename
            })







if __name__ == "__main__":
    main()