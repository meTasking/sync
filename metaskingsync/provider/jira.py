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

    unprocessed: list[DataPointAction]
    failed: list[DataPointAction]

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

                        start = datetime.fromisoformat(
                            record["started"]
                        ).astimezone()
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

    def apply_changes(self, changes: Iterable[DataPointAction]):
        for change in changes:
            try:
                self._apply_change(change)
            except Exception:
                self.failed.append(change)
                import traceback
                traceback.print_exc()

    def _apply_change(self, change: DataPointAction):
        if change.is_update:
            assert change.prev is not None
            assert change.next is not None
            if change.prev.name != change.next.name:
                # Edge case: name change
                # Delete old worklog and create new one by splitting
                # the change into two separate changes
                self._apply_change(DataPointAction(
                    prev=change.prev,
                ))
                self._apply_change(DataPointAction(
                    next=change.next,
                ))
                return

        if change.is_delete:
            assert change.prev is not None

            if not self.allow_delete:
                return

            assert change.prev.id.startswith("http"), change.prev.id

            # DELETE
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog/X

            # Delete existing worklog
            response = requests.delete(
                change.prev.id,
                auth=(self.username, self.token),
            )
            response.raise_for_status()
            return

        if change.is_create:
            assert change.next is not None

            # POST
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog
            # {"timeSpent":"60m","comment":"text","started":"2023-09-29T12:10:16.723+0200"}

            # Check if name is valid issue key
            if not ISSUE_KEY_REGEX.match(change.next.name):
                self.unprocessed.append(change)
                from rich import print
                print(
                    f"[red]Invalid issue key: {change.next.name}[/red]",
                    file=sys.stderr
                )
                return

            start_time = change.next.start
            if start_time.tzinfo is None:
                start_time = start_time.astimezone()

            spent_seconds = (
                change.next.end - change.next.start
            ).total_seconds()
            spent_overflow = spent_seconds % 60
            end_time_seconds = (
                change.next.end.second +
                (change.next.end.microsecond / 1000000.0)
            )
            if end_time_seconds - spent_overflow < 0:
                # Fix for stupid Jira supporting only minutes
                spent_seconds += 60

            # Create new worklog
            response = requests.post(
                URL_ISSUE_WORKLOG % (self.server, change.next.name),
                auth=(self.username, self.token),
                json={
                    "comment": change.next.description,
                    "started": start_time.strftime("%Y-%m-%dT%H:%M:%S.000%z"),
                    "timeSpentSeconds": int(spent_seconds),
                },
            )
            if response.status_code == 404 or response.status_code == 400:
                self.unprocessed.append(change)
                from rich import print
                print(
                    f"[red]Issue not found: {change.next.name}[/red]",
                    file=sys.stderr
                )
                print(response.json(), file=sys.stderr)
                return

            response.raise_for_status()
            return

        if change.is_update:
            assert change.prev is not None
            assert change.next is not None

            assert change.prev.id.startswith("http"), change.prev.id
            assert change.next.id.startswith("http"), change.next.id

            assert change.prev.id == change.next.id, (
                change.prev.id,
                change.next.id,
            )
            assert change.prev.name == change.next.name, (
                "Name change should have been handled earlier"
            )

            # PUT
            # https://domain.atlassian.net/rest/api/2/issue/CODE-XX/worklog/X
            # {"timeSpent":"15m","started":"2023-09-21T11:15:00.000+0200","comment":"text"}

            start_time = change.next.start
            if start_time.tzinfo is None:
                start_time = start_time.astimezone()

            spent_seconds = (
                change.next.end - change.next.start
            ).total_seconds()
            spent_overflow = spent_seconds % 60
            end_time_seconds = change.next.end.second
            if end_time_seconds - spent_overflow < 0:
                # Fix for stupid Jira supporting only minutes
                spent_seconds += 60

            # Update existing worklog
            response = requests.put(
                change.next.id,
                auth=(self.username, self.token),
                json={
                    "comment": change.next.description,
                    "started": start_time.strftime("%Y-%m-%dT%H:%M:%S.000%z"),
                    "timeSpentSeconds": int(spent_seconds),
                },
            )
            response.raise_for_status()
            return

    def report(self) -> dict[str, Any]:
        base = super().report()
        base["Failed"] = self.failed
        base["Unprocessed"] = self.unprocessed
        return base
