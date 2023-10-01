from datetime import datetime
from typing import Iterable
import requests
import re

from .base import BaseProvider, DataPoint, DataPointAction


URL_ENTRIES_ALL = "https://www.toggl.com/api/v9/me/time_entries"
URL_ENTRY_CREATE = "https://www.toggl.com/api/v9/workspaces/%s/time_entries"
URL_ENTRY_UPDATE = "https://www.toggl.com/api/v9/workspaces/%s/time_entries/%s"

NAME_REGEX = re.compile(r"^(.*)[: -_](.*)$")


class TogglProvider(BaseProvider):

    token: str
    workspace_id: str
    split_name: bool

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
        token: str,
        workspace_id: str,
        split_name: bool,
    ):
        self.token = token
        self.workspace_id = workspace_id
        self.split_name = split_name

        super().__init__(since, until, dry_run, allow_delete)

    def initialize_data_points(self) -> Iterable[DataPoint]:
        params: dict[str, str] = {}
        if self.since is not None:
            params["start_date"] = self.since.isoformat()
        if self.until is not None:
            params["end_date"] = self.until.isoformat()

        response = requests.get(
            URL_ENTRIES_ALL,
            params=params,
            auth=(self.token, "api_token"),
        )
        response.raise_for_status()
        entries = response.json()

        for entry in entries:
            if entry["workspace_id"] != self.workspace_id:
                continue

            name_match = (
                NAME_REGEX.match(entry["description"])
                if self.split_name else
                None
            )
            if name_match is not None:
                name = name_match.group(1).strip()
                description = name_match.group(2).strip()
            else:
                name = entry["description"]
                description = None
            yield DataPoint(
                id=str(entry['id']),
                name=name,
                description=description,
                start=datetime.fromisoformat(entry["start"]),
                end=datetime.fromisoformat(entry["stop"]),
            )

    def apply_changes(self, changes: Iterable[DataPoint]):
        for change in changes:
            if change.action == DataPointAction.DELETE:
                if not self.allow_delete:
                    continue
                response = requests.delete(
                    URL_ENTRY_UPDATE % (self.workspace_id, change.id),
                    auth=(self.token, "api_token"),
                )
                response.raise_for_status()
                continue

            if change.action == DataPointAction.UPDATE:
                if not self.split_name or change.description is None:
                    description = change.name
                else:
                    description = f"{change.name}: {change.description}"
                response = requests.put(
                    URL_ENTRY_UPDATE % (self.workspace_id, change.id),
                    auth=(self.token, "api_token"),
                    json={
                        "description": description,
                        "start": change.start.isoformat(),
                        "stop": change.end.isoformat(),
                    },
                )
                response.raise_for_status()
                continue

            if change.action == DataPointAction.CREATE:
                if not self.split_name or change.description is None:
                    description = change.name
                else:
                    description = f"{change.name}: {change.description}"
                response = requests.post(
                    URL_ENTRY_CREATE % self.workspace_id,
                    auth=(self.token, "api_token"),
                    json={
                        "created_with": "meTasking SYNC",
                        "workspace_id": self.workspace_id,
                        "description": description,
                        "start": change.start.isoformat(),
                        "stop": change.end.isoformat(),
                    },
                )
                response.raise_for_status()
                continue
