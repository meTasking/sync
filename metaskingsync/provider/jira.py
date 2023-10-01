import sys
from datetime import datetime, timedelta
from typing import Iterable, Any
import requests
import re

from .base import BaseProvider, DataPoint, DataPointAction


URL_USER = "%s/rest/auth/1/session"
URL_QUERY_ISSUES = "%s/rest/api/2/search"
URL_ISSUE_WORKLOG = "%s/rest/api/2/issue/%s/worklog"
URL_ISSUE_WORKLOG_SUFFIX = "%s/worklog"

ISSUE_KEY_REGEX = re.compile(r"^[A-Z]+-\d+$")


class JiraProvider(BaseProvider):

    server: str
    username: str
    token: str

    unprocessed: list[DataPoint]
    failed: list[DataPoint]

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
        server: str,
        username: str,
        token: str
    ):
        self.server = server
        self.username = username
        self.token = token

        self.unprocessed = []
        self.failed = []

        super().__init__(since, until, dry_run, allow_delete)

    def _user_id(self) -> str:
        response = requests.get(
            URL_USER % self.server,
            auth=(self.username, self.token),
        )
        response.raise_for_status()
        return response.json()["name"]

    def initialize_data_points(self) -> Iterable[DataPoint]:
        user_id = self._user_id()

        # The jql date filter does not have to be exact, it just has to be
        # close enough to get all the records in the range. They will be
        # filtered more precisely by indexer in BaseProvider.
        jql = "worklogAuthor = currentUser()"
        if self.since is not None:
            jql += f" AND worklogDate >= \"{self.since.date().isoformat()}\""
        if self.until is not None:
            jql += f" AND worklogDate <= \"{self.until.date().isoformat()}\""

        offset = 0
        while True:
            response = requests.post(
                URL_QUERY_ISSUES % self.server,
                auth=(self.username, self.token),
                json={
                    "fields": ["worklog", "summary"],
                    "startAt": offset,
                    "maxResults": 250,
                    "jql": jql,
                }
            )
            response.raise_for_status()
            issues = response.json()

            for issue in issues["issues"]:
                records = issue["fields"]["worklog"]
                offset_records = records["startAt"]
                while True:
                    record: dict[str, Any]
                    for record in records["worklogs"]:
                        if record["author"]["accountId"] != user_id:
                            continue

                        start = datetime.fromisoformat(record["started"])
                        end_delta = timedelta(
                            seconds=int(record["timeSpentSeconds"])
                        )
                        yield DataPoint(
                            id=str(record['self']),
                            name=issue["key"],
                            description=(
                                record["comment"]
                                if "comment" in record else
                                None
                            ),
                            start=start,
                            end=start + end_delta,
                        )

                    if records["maxResults"] + records["startAt"] \
                            >= records["total"]:
                        break

                    offset_records += records["maxResults"]

                    response = requests.get(
                        URL_ISSUE_WORKLOG_SUFFIX % issue["self"],
                        auth=(self.username, self.token),
                        params={
                            "startAt": offset_records,
                            "maxResults": 250,
                        }
                    )
                    response.raise_for_status()
                    records = response.json()

            if issues["maxResults"] + issues["startAt"] >= issues["total"]:
                break

            offset += issues["maxResults"]

    def apply_changes(self, changes: Iterable[DataPoint]):
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

            assert change.id.startswith("http"), change.id

            # DELETE
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog/X

            # Delete existing worklog
            response = requests.delete(
                change.id,
                auth=(self.username, self.token),
            )
            response.raise_for_status()
            return

        if change.action == DataPointAction.CREATE:

            # POST
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog
            # {"timeSpent":"60m","comment":"text","started":"2023-09-29T12:10:16.723+0200"}

            # Check if name is valid issue key
            if not ISSUE_KEY_REGEX.match(change.name):
                self.unprocessed.append(change)
                from rich import print
                print(
                    f"[red]Invalid issue key: {change.name}[/red]",
                    file=sys.stderr
                )
                return

            # Create new worklog
            response = requests.post(
                URL_ISSUE_WORKLOG % (self.server, change.name),
                auth=(self.username, self.token),
                json={
                    "comment": change.description,
                    "started": change.start.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                    "timeSpentSeconds": int((
                        change.end - change.start
                    ).total_seconds()),
                },
            )
            if response.status_code == 404 or response.status_code == 400:
                self.unprocessed.append(change)
                from rich import print
                print(
                    f"[red]Issue not found: {change.name}[/red]",
                    file=sys.stderr
                )
                print(response.json(), file=sys.stderr)
                return

            response.raise_for_status()
            return

        if change.action == DataPointAction.UPDATE:
            assert change.id.startswith("http"), change.id

            # PUT
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog/X
            # {"timeSpent":"15m","started":"2023-09-21T11:15:00.000+0200","comment":"text"}

            # FIXME: Changing name (parent issue) does not work

            # Update existing worklog
            response = requests.put(
                change.id,
                auth=(self.username, self.token),
                json={
                    "comment": change.description,
                    "started": change.start.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                    "timeSpentSeconds": (
                        change.end - change.start
                    ).total_seconds(),
                },
            )
            response.raise_for_status()
            return

    def report(self) -> dict[str, Any]:
        base = super().report()
        base["Failed"] = self.failed
        base["Unprocessed"] = self.unprocessed
        return base
