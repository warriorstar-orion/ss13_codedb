from pathlib import Path
import tomllib

import click
from loguru import logger
from sqlalchemy import create_engine

from ss13_codedb.git_models import Base


@click.command()
@click.option("--settings", required=True, type=Path)
def main(settings: Path):
    logger.info("Attempting to create database tables.")
    config = tomllib.load(open(settings, 'rb'))
    engine = create_engine(config["config"]["db_connection_string"])

    Base.metadata.create_all(engine)
    logger.info("Created database tables.")


if __name__ == "__main__":
    main()
