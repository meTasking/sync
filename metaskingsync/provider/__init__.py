import sys

from metaskingsync.args import DataProvider, CliArgs
from .base import Provider, DataPoint, DataPointAction
from .metasking import MetaTaskingProvider
from .jira import JiraProvider
from .toggl import TogglProvider
from .json import JsonProvider


def select_provider(provider: DataProvider, args: CliArgs):
    if provider == DataProvider.metasking:
        return MetaTaskingProvider(
            args.since,
            args.until,
            args.dry_run,
            args.delete,
            args.metasking_server,
        )
    elif provider == DataProvider.jira:
        assert args.jira_server is not None, \
            "Jira server is required for Jira provider"
        assert args.jira_username is not None, \
            "Jira username is required for Jira provider"
        return JiraProvider(
            args.since,
            args.until,
            args.dry_run,
            args.delete,
            args.jira_server,
            args.jira_username,
            args.obtain_jira_token(),
        )
    elif provider == DataProvider.toggl:
        assert args.toggl_token is not None, \
            "Toggl token is required for Toggl provider"
        assert args.toggl_workspace_id is not None, \
            "Toggl workspace ID is required for Toggl provider"
        return TogglProvider(
            args.since,
            args.until,
            args.dry_run,
            args.delete,
            args.toggl_token,
            args.toggl_workspace_id,
            args.toggl_split_name,
        )
    elif provider == DataProvider.json:
        return JsonProvider(
            args.since,
            args.until,
            args.dry_run,
            args.delete,
            None if args.json_no_input else sys.stdin,
            None if args.json_no_output else sys.stdout,
            args.json_only_modifications,
        )
    else:
        raise ValueError(f"Unknown provider: {provider}")


__all__ = [
    "Provider",
    "DataPoint",
    "DataPointAction",
    "select_provider",
    "MetaTaskingProvider",
    "JiraProvider",
    "TogglProvider",
    "JsonProvider",
]
