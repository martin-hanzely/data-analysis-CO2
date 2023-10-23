import logging

import typer

from data.tasks import debug_task


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Include datetime for easier execution time tracking.
)

app = typer.Typer()


@app.command()
def debug_command() -> None:
    """
    Debug command.
    """
    logging.info(f"{debug_task()=}")


if __name__ == "__main__":
    app()
