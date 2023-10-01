import os
import enum
from datetime import datetime
from typing import Optional
from dateutil import parser
from pydantic import BaseModel, Field, validator


class DataProvider(enum.Enum):
    metasking = "metasking"
    jira = "jira"
    toggl = "toggl"
    json = "json"


class Accuracy(enum.Enum):
    minute = "minute"
    second = "second"
    microsecond = "microsecond"


class CliArgs(BaseModel):
    metasking_server: str = Field(
        default=os.environ.get("METASKING_SERVER", "http://localhost:8000"),
        description="meTasking server address",
    )

    jira_server: str = Field(
        default=os.environ.get(
            "ATLASSIAN_JIRA_SERVER",
            "https://atlassian.net"
        ),
        description="Atlassian jira server address",
    )
    jira_username: Optional[str] = Field(
        default=os.environ.get("ATLASSIAN_JIRA_USERNAME"),
        description="Atlassian jira username",
    )
    jira_token: Optional[str] = Field(
        default=os.environ.get("ATLASSIAN_JIRA_TOKEN"),
        description="Atlassian jira token",
    )

    toggl_token: Optional[str] = Field(
        default=os.environ.get("TOGGL_TOKEN"),
        description="Toggl token",
    )
    toggl_workspace_id: Optional[str] = Field(
        default=os.environ.get("TOGGL_WORKSPACE_ID"),
        description="Toggl workspace id",
    )
    toggl_split_name: bool = Field(
        default=False,
        description=(
            "split description of toggl time entry into name and description"
        ),
    )

    json_no_input: bool = Field(
        default=False,
        description="do not read any data points from standard input",
    )
    json_no_output: bool = Field(
        default=False,
        description="do not write any data points to standard output",
    )
    json_only_modifications: bool = Field(
        default=False,
        description=(
            "write modified data points to standard " +
            "output instead of all data points"
        ),
    )

    verbose: bool = Field(
        default=False,
        description="enable output of logged debug",
    )

    delete: bool = Field(
        default=True,
        description=(
            "also delete data points from target " +
            "that are not present in source"
        ),
    )

    dry_run: bool = Field(
        default=False,
        description="do not perform any changes",
    )

    source: DataProvider = Field(
        default=DataProvider.metasking,
        description="source of data",
    )

    target: DataProvider = Field(
        default=DataProvider.jira,
        description="target of data",
    )

    accuracy: Accuracy = Field(
        default=Accuracy.minute,
        description=(
            "accuracy of data synchronization " +
            "(all data points are rounded to this accuracy before comparison)"
        ),
    )

    since: Optional[datetime] = Field(
        default=None,
        description="only sync data since this date",
    )
    until: Optional[datetime] = Field(
        default=None,
        description="only sync data until this date",
    )

    @validator("since", "until", pre=True, always=True)
    def parse_datetime(cls, value):
        if isinstance(value, datetime):
            return value  # If it's already a datetime object, return it as is

        try:
            return parser.parse(value)
        except Exception as e:
            raise ValueError(f"Failed to parse datetime: {e}")

    class Config:
        validate_all = True
