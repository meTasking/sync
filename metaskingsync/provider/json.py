from datetime import datetime
from typing import Iterable, TextIO, Optional

import jsonstream

from .base import BaseProvider, DataPoint, DataPointAction


class JsonProvider(BaseProvider):
    input: Optional[TextIO]
    output: Optional[TextIO]
    output_only_modifications: bool

    def __init__(
        self,
        since: datetime | None,
        until: datetime | None,
        dry_run: bool,
        allow_delete: bool,
        input: Optional[TextIO],
        output: Optional[TextIO],
        output_only_modifications: bool,
    ):
        self.input = input
        self.output = output
        self.output_only_modifications = output_only_modifications

        super().__init__(since, until, dry_run, allow_delete)

    def initialize_data_points(self) -> Iterable[DataPoint]:
        if self.input is None:
            return

        for data in jsonstream.load(self.input):
            assert isinstance(data, dict)
            assert "action" not in data, \
                "Field 'action' is not allowed to be set in input data"
            data_point = DataPoint.parse_obj(data)
            yield data_point

    def apply_changes(self, changes: Iterable[DataPoint]):
        if self.output is None:
            return

        if self.output_only_modifications:
            for change in changes:
                if change.action == DataPointAction.DELETE:
                    if not self.allow_delete:
                        continue
                    jsonstream.dump({
                        "id": change.id,
                        "action": DataPointAction.DELETE,
                    }, self.output)
                elif change.action == DataPointAction.CREATE:
                    self.output.write(change.json(exclude={"id"}))
                    self.output.write("\n")
                elif change.action == DataPointAction.UPDATE:
                    self.output.write(change.json())
                    self.output.write("\n")
                elif change.action == DataPointAction.NONE:
                    pass
                else:
                    raise ValueError(
                        f"Unknown action: {change.action}"
                    )
        else:
            for id in self.data_sequence:
                data_point = self.data_map[id]
                exclude = {"action"}
                if data_point.action == DataPointAction.CREATE:
                    exclude.add("id")
                self.output.write(data_point.json(exclude=exclude))
                self.output.write("\n")
