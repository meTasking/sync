from typing import Iterable, Any
from datetime import datetime
from abc import ABC, abstractmethod
import enum

from pydantic import BaseModel, Field, validator


class DataPointAction(enum.StrEnum):
    CREATE = "create"
    DELETE = "delete"
    UPDATE = "update"
    NONE = "none"


class DataPoint(BaseModel):
    id: str = Field(
        description=(
            "unique identifier of the data point (can be anything); " +
            "used to allow overwriting of existing data points"
        ),
    )
    action: DataPointAction = Field(
        default=DataPointAction.NONE,
        description="action to be taken on the data point",
    )
    name: str = Field(
        description="identifier of the data point - used for grouping",
    )
    description: str | None = Field(
        default=None,
        description="description of the data point - can be anything",
    )
    start: datetime = Field(
        description="start of work",
    )
    end: datetime = Field(
        description="end of work",
    )

    @validator("end")
    def end_must_be_after_start(cls, v, values):
        if v < values["start"]:
            raise ValueError("end must be after start")
        return v


class Provider(ABC):

    since: datetime | None
    until: datetime | None
    dry_run: bool
    allow_delete: bool

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
    ) -> None:
        if since is not None and until is not None and since > until:
            raise ValueError("since must be before until")

        self.since = since
        self.until = until
        self.dry_run = dry_run
        self.allow_delete = allow_delete

        super().__init__()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.apply()

    @abstractmethod
    def dump(self) -> Iterable[DataPoint]:
        pass

    @abstractmethod
    def add_changes(self, data: Iterable[DataPoint]):
        pass

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def apply(self):
        pass

    @abstractmethod
    def report(self) -> Any:
        pass


class BaseProvider(Provider):

    data_sequence: list[str]
    data_indexes: dict[str, int]
    data_map: dict[str, DataPoint]
    remaining_changes: list[DataPoint]
    all_changes: list[DataPoint]

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
    ):
        self.data_sequence = []
        self.data_indexes = {}
        self.data_map = {}
        self.remaining_changes = []
        self.all_changes = []

        super().__init__(since, until, dry_run, allow_delete)

    def open(self):
        for data_point in self.initialize_data_points():
            self.index_data_point(data_point)

    def index_data_point(self, data_point: DataPoint):
        if self.since is not None and data_point.end < self.since:
            return

        if self.until is not None and data_point.start > self.until:
            return

        if data_point.id in self.data_map:
            raise ValueError(f"Duplicate data point id: {data_point.id}")

        self.data_sequence.append(data_point.id)
        self.data_indexes[data_point.id] = \
            len(self.data_sequence) - 1
        self.data_map[data_point.id] = data_point

    @abstractmethod
    def initialize_data_points(self) -> Iterable[DataPoint]:
        pass

    def dump(self) -> Iterable[DataPoint]:
        for id in self.data_sequence:
            yield self.data_map[id]

    def add_changes(self, data: Iterable[DataPoint]):
        self.remaining_changes += data
        self.all_changes += data
        for data_point in data:
            if data_point.action == DataPointAction.CREATE:
                if data_point.id in self.data_map:
                    raise ValueError(
                        f"Duplicate data point id: {data_point.id}"
                    )
                else:
                    self.data_sequence.append(data_point.id)
                    self.data_indexes[data_point.id] = \
                        len(self.data_sequence) - 1
                    self.data_map[data_point.id] = data_point
            elif data_point.action == DataPointAction.DELETE:
                if data_point.id in self.data_map:
                    data_point_index = self.data_indexes[data_point.id]
                    self.data_sequence.pop(data_point_index)
                    for id in self.data_sequence[data_point_index:]:
                        self.data_indexes[id] -= 1
                    del self.data_indexes[data_point.id]
                    del self.data_map[data_point.id]
            elif data_point.action == DataPointAction.UPDATE:
                self.data_map[data_point.id] = data_point
            elif data_point.action == DataPointAction.NONE:
                pass
            else:
                raise ValueError(f"Unknown action: {data_point.action}")
        return super().add_changes(data)

    def apply(self):
        if not self.dry_run:
            self.apply_changes(self.remaining_changes)
        self.remaining_changes = []

    @abstractmethod
    def apply_changes(self, changes: Iterable[DataPoint]):
        pass

    def report(self) -> dict[str, Any]:
        return {
            "Deleted": list(filter(
                lambda x: x.action == DataPointAction.DELETE,
                self.all_changes,
            )),
            "Added": list(filter(
                lambda x: x.action == DataPointAction.CREATE,
                self.all_changes,
            )),
            "Updated": list(filter(
                lambda x: x.action == DataPointAction.UPDATE,
                self.all_changes,
            )),
            "Unchanged but added to changed (should be empty)": list(filter(
                lambda x: x.action == DataPointAction.NONE,
                self.all_changes,
            )),
        }
