import os
import click
import pandas as pd
from flask import Flask, send_from_directory
from pyngrok import ngrok

from urllib.parse import urljoin


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--path",
    required=True,
    type=click.Path(dir_okay=True, file_okay=False, exists=True),
)
@click.option("--port", default=5000, type=int)
@click.option("--csv-outfile", required=True, type=click.Path(file_okay=True))
def host_ngrok(path, csv_outfile, port=5000):
    """
    Serve images from a directory and create an ngrok tunnel to access them publicly.
    """

    try:
        df = pd.read_csv(os.path.join(path, "index.csv")).dropna(
            subset=["savedFilename"]
        )
    except:
        print(
            f"No file found {os.path.join(path, 'index.csv')}: Please specify a path to an images dir generated by the export_images.py which has an index.csv in it."
        )
        return

    app = Flask(__name__)

    if not os.path.isdir(path):
        click.echo(f"The specified path {path} is not a directory.")
        return

    @app.route("/<filename>")
    def serve_image(filename):
        return send_from_directory(path, filename)

    # Start ngrok tunnel
    tunnel = ngrok.connect(port)

    click.echo(f"Public URL: {tunnel.public_url}")

    # Display URLs for each image
    df["uri"] = df.savedFilename.apply(lambda x: urljoin(tunnel.public_url, x))

    df.to_csv(csv_outfile)

    app.run(port=port)

    print("CSV updated to include Ngrok URLS...")


if __name__ == "__main__":
    cli()
