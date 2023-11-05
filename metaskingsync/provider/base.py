from typing import Iterable, Any
from datetime import datetime
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field, validator, root_validator


class DataPoint(BaseModel):
    id: str = Field(
        description=(
            "unique identifier of the data point (can be anything); " +
            "used to allow overwriting of existing data points"
        ),
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


class DataPointAction(BaseModel):
    prev: DataPoint | None = Field(
        default=None,
        description="previous version of the data point",
    )
    next: DataPoint | None = Field(
        default=None,
        description="next version of the data point",
    )

    @property
    def is_create(self) -> bool:
        return self.prev is None and self.next is not None

    @property
    def is_update(self) -> bool:
        return self.prev is not None and self.next is not None

    @property
    def is_delete(self) -> bool:
        return self.prev is not None and self.next is None

    @root_validator
    def validate_action(cls, values):
        prev = values.get("prev")
        next = values.get("next")
        if prev is None and next is None:
            raise ValueError("prev and next cannot both be None")
        return values


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
    def add_changes(self, data: Iterable[DataPointAction]):
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
    remaining_changes: list[DataPointAction]
    all_changes: list[DataPointAction]

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

    def add_changes(self, data: Iterable[DataPointAction]):
        self.remaining_changes += data
        self.all_changes += data
        for data_point in data:
            if data_point.is_create:
                assert data_point.next is not None
                if data_point.next.id in self.data_map:
                    raise ValueError(
                        f"Duplicate data point id: {data_point.next.id}"
                    )
                else:
                    self.data_sequence.append(data_point.next.id)
                    self.data_indexes[data_point.next.id] = \
                        len(self.data_sequence) - 1
                    self.data_map[data_point.next.id] = data_point.next
            elif data_point.is_delete:
                assert data_point.prev is not None
                if data_point.prev.id in self.data_map:
                    data_point_index = self.data_indexes[data_point.prev.id]
                    self.data_sequence.pop(data_point_index)
                    for id in self.data_sequence[data_point_index:]:
                        self.data_indexes[id] -= 1
                    del self.data_indexes[data_point.prev.id]
                    del self.data_map[data_point.prev.id]
            elif data_point.is_update:
                assert data_point.next is not None
                self.data_map[data_point.next.id] = data_point.next
            else:
                raise ValueError(f"Unknown action: {data_point}")
        return super().add_changes(data)

    def apply(self):
        if not self.dry_run:
            self.apply_changes(self.remaining_changes)
        self.remaining_changes = []

    @abstractmethod
    def apply_changes(self, changes: Iterable[DataPointAction]):
        pass

    def report(self) -> dict[str, Any]:
        return {
            "Added": list(filter(
                lambda x: x.prev is None and x.next is not None,
                self.all_changes,
            )),
            "Updated": list(filter(
                lambda x: x.prev is not None and x.next is not None,
                self.all_changes,
            )),
            "Deleted": list(filter(
                lambda x: x.prev is not None and x.next is None,
                self.all_changes,
            )),
            "Invalid": list(filter(
                lambda x: x.prev is None and x.next is None,
                self.all_changes,
            )),
        }
