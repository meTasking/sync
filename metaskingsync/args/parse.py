from typing import Optional
from datetime import datetime

from pydantic_argparse.argparse.parser import ArgumentParser

from .model import CliArgs

from metaskingsync._version import __version__


def _post_process_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None

    # If the datetime is not timezone aware, assume current timezone
    if value.tzinfo is None:
        value = value.astimezone()

    return value


def parse_arguments(
    program_args: Optional[list[str]] = None,
) -> tuple[ArgumentParser, CliArgs]:
    parser = ArgumentParser(
        model=CliArgs,
        prog="metask-sync",
        description=(
            "meTasking SYNC - " +
            "Sync your work time logging " +
            "across different platforms from CLI"
        ),
        version=__version__,
    )
    args: CliArgs = parser.parse_typed_args(args=program_args)

    args.since = _post_process_datetime(args.since)
    args.until = _post_process_datetime(args.until)

    return parser, args
