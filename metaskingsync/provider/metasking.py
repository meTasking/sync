from datetime import datetime
from typing import Iterable, Any
import requests

from .base import BaseProvider, DataPoint, DataPointAction


API_VERSION = "v1"
URL_LOG = f"/api/{API_VERSION}/log"
URL_LOG_LIST = f"{URL_LOG}/list"
URL_RECORD = f"/api/{API_VERSION}/record"


class MetaTaskingProvider(BaseProvider):

    server: str

    failed: list[DataPoint]

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
        server: str
    ):
        self.server = server

        self.failed = []

        super().__init__(since, until, dry_run, allow_delete)

    def initialize_data_points(self) -> Iterable[DataPoint]:
        offset = 0
        while True:
            response = requests.get(
                f"{self.server}{URL_LOG_LIST}",
                params={
                    "offset": offset,
                    "limit": 100,
                },
            )
            response.raise_for_status()
            logs = response.json()

            if len(logs) == 0:
                break

            for log in logs:
                for record in log["records"]:
                    if record["end"] is None:
                        continue

                    has_task = log["task"] is not None
                    name = (
                        log["task"]["name"]
                        if has_task else
                        log["name"]
                    )
                    description = (
                        log["name"] + ": " + log["description"]
                        if has_task else
                        log["description"]
                    )
                    yield DataPoint(
                        id=str(record['id']),
                        name=name,
                        description=description,
                        start=datetime.fromisoformat(
                            record["start"]
                        ).astimezone(),
                        end=datetime.fromisoformat(
                            record["end"]
                        ).astimezone(),
                    )

            offset += len(logs)

    def apply_changes(self, changes: Iterable[DataPoint]):
        # NOTE: It would be ideal to handle this transactionally,
        #       but that is not going to happen...

        for change in changes:
            try:
                self._apply_change(change)
            except Exception:
                self.failed.append(change)
                import traceback
                traceback.print_exc()

    def _apply_change(self, change: DataPoint):
        if change.action == DataPointAction.DELETE:
            if not self.allow_delete:
                return
            # Delete existing record
            response = requests.delete(
                f"{self.server}{URL_RECORD}/{change.id}",
            )
            response.raise_for_status()
            return

        if change.action == DataPointAction.CREATE:
            # Create new log
            response = requests.post(
                f"{self.server}{URL_LOG}",
                json={
                    "name": change.name,
                    "description": change.description,
                    "records": [
                        {
                            "start": change.start.isoformat(),
                            "end": change.end.isoformat(),
                        },
                    ],
                },
            )
            response.raise_for_status()
            return

        if change.action == DataPointAction.UPDATE:
            # Update existing log and record
            response = requests.put(
                f"{self.server}{URL_RECORD}/{change.id}",
                json={
                    "start": change.start.isoformat(),
                    "end": change.end.isoformat(),
                },
            )
            response.raise_for_status()
            response = requests.get(
                f"{self.server}{URL_RECORD}/{change.id}/log",
            )
            response.raise_for_status()
            log = response.json()
            response = requests.put(
                f"{self.server}{URL_LOG}/{log['id']}",
                json={
                    "name": change.name,
                    "description": change.description,
                },
            )
            response.raise_for_status()
            return

    def report(self) -> dict[str, Any]:
        base = super().report()
        base["Failed"] = self.failed
        return base
