"""Console script for DB_Connector_Pro."""

import typer
from rich.console import Console

from DB_Connector_Pro_template import utils

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for DB_Connector_Pro."""
    console.print("Replace this message by putting your code into "
               "DB_Connector_Pro.cli.main")
    console.print("See Typer documentation at https://typer.tiangolo.com/")
    utils.do_something_useful()


if __name__ == "__main__":
    app()
