import sys
import logging

from rich.console import Console

from .args import parse_arguments
from .provider import select_provider
from .sync import sync

root_log = logging.getLogger()
log = logging.getLogger(__name__)


def setup_log() -> logging.Handler:
    root_log.setLevel(logging.DEBUG)

    root_stderr_handler = logging.StreamHandler(stream=sys.stderr)
    root_stderr_handler.setLevel(logging.INFO)
    basic_formatter = logging.Formatter(
        "%(asctime)s\t-\t%(name)s\t-\t%(levelname)s\t-\t%(message)s"
    )
    root_stderr_handler.setFormatter(basic_formatter)
    root_log.addHandler(root_stderr_handler)
    return root_stderr_handler


def main():
    # Configure logging
    root_log_handler = setup_log()

    parser, args = parse_arguments()

    # Apply logging related arguments
    if args.verbose:
        root_log_handler.setLevel(logging.DEBUG)

    if args.since is None and args.until is None:
        print(
            "NOTE: It is recommended to specify --since and --until " +
            "arguments to limit the amount of data to be processed.",
            file=sys.stderr
        )
        print(
            "NOTE: All data comparisons are done in memory, " +
            "using bigger windows than necessary may slow down " +
            "the process.",
            file=sys.stderr
        )

    if args.source == args.target:
        parser.error(
            "Syncing from same source platform and the same " +
            "target platform is not supported at the moment."
        )

    # Get the providers
    tgt = None
    try:
        with select_provider(args.source, args) as source_provider:
            with select_provider(args.target, args) as target_provider:
                tgt = target_provider

                # Synchronize the data
                sync(args.accuracy, source_provider, target_provider)
    finally:
        # Print the report of changes
        if tgt is not None:
            console = Console(file=sys.stderr)
            console.print(tgt.report())
