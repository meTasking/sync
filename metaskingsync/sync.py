from datetime import datetime

from .args import Accuracy
from .provider import Provider, DataPoint, DataPointAction


_SOURCE = 0
_DESTINATION = 1


def round_datetime(accuracy: Accuracy, dt: datetime) -> datetime:
    if accuracy == Accuracy.minute:
        return dt.replace(second=0, microsecond=0)
    elif accuracy == Accuracy.second:
        return dt.replace(microsecond=0)
    elif accuracy == Accuracy.microsecond:
        return dt
    else:
        raise ValueError(f"Unknown accuracy: {accuracy}")


def sync(accuracy: Accuracy, source: Provider, destination: Provider):
    id_map: dict[tuple[int, str], DataPoint] = {}
    time_start_map: dict[datetime, set[tuple[int, str]]] = {}
    time_end_map: dict[datetime, set[tuple[int, str]]] = {}

    ids_list: tuple[list[str], list[str]] = ([], [])

    next_generated_id = 0

    def next_id() -> str:
        nonlocal next_generated_id
        next_id = "new-" + str(next_generated_id)
        while (
            (_SOURCE, next_id) in id_map or
            (_DESTINATION, next_id) in id_map
        ):
            next_generated_id += 1
            next_id = "new-" + str(next_generated_id)
        next_generated_id += 1
        return next_id

    def index_data_point(data_type: int, data_point: DataPoint):
        if (data_type, data_point.id) in id_map:
            raise ValueError(f"Duplicate data point id: {data_point.id}")
        id_map[(data_type, data_point.id)] = data_point

        rounded_start = round_datetime(accuracy, data_point.start)
        rounded_end = round_datetime(accuracy, data_point.end)
        for (data_type2, id2) in time_start_map.get(rounded_start, set()):
            if data_type2 != data_type:
                continue
            data_point2 = id_map[(data_type2, id2)]
            if data_point2.end != data_point.end:
                continue
            raise ValueError(
                "Duplicate data point (both data points have " +
                "the same rounded start and end time, this is not supported " +
                "as the algorithm uses rounded start and end time as data " +
                "point fingerprint for matching):" +
                f" {data_point.id} and {data_point2.id}"
            )
        time_start_map.setdefault(rounded_start, set()) \
            .add((data_type, data_point.id))
        time_end_map.setdefault(rounded_end, set()) \
            .add((data_type, data_point.id))
        ids_list[data_type].append(data_point.id)

    for data_point in source.dump():
        index_data_point(_SOURCE, data_point)

    for data_point in destination.dump():
        index_data_point(_DESTINATION, data_point)

    missing_in_destination: set[str] = set()
    for id in ids_list[_SOURCE]:
        data_point = id_map[(_SOURCE, id)]
        rounded_start = round_datetime(accuracy, data_point.start)
        rounded_end = round_datetime(accuracy, data_point.end)
        for data_type, sid in time_start_map.get(rounded_start, set()):
            if data_type != _DESTINATION:
                continue

            destination_data_point = id_map[(data_type, sid)]
            destination_rounded_end = \
                round_datetime(accuracy, destination_data_point.end)
            if rounded_end != destination_rounded_end:
                continue

            break
        else:
            # break not called
            missing_in_destination.add(id)

    modified_in_destination_ids: set[str] = set()
    modified_in_destination: list[tuple[DataPoint, DataPoint]] = list()
    additional_in_destination: set[str] = set()
    for id in ids_list[_DESTINATION]:
        data_point = id_map[(_DESTINATION, id)]
        rounded_start = round_datetime(accuracy, data_point.start)
        rounded_end = round_datetime(accuracy, data_point.end)
        for data_type, sid in time_start_map.get(rounded_start, set()):
            if data_type != _SOURCE:
                continue

            source_data_point = id_map[(data_type, sid)]
            source_rounded_end = \
                round_datetime(accuracy, source_data_point.end)
            if rounded_end != source_rounded_end:
                continue

            if (
                data_point.name != source_data_point.name or
                data_point.description != source_data_point.description
            ):
                # Target data point is different from source data point
                # Add it as a modification
                if id in modified_in_destination_ids:
                    raise ValueError(
                        "Same id matched multiple times as being " +
                        f"different from source: {data_point.id}"
                    )
                modified_in_destination_ids.add(id)
                modified_data_point = data_point.copy()
                modified_data_point.name = source_data_point.name
                modified_data_point.description = \
                    source_data_point.description
                modified_in_destination.append(
                    (data_point, modified_data_point)
                )

            break
        else:
            # break not called
            additional_in_destination.add(id)

    modifications: list[DataPointAction] = []

    for data_point_prev, data_point_next in modified_in_destination:
        modifications.append(DataPointAction(
            prev=data_point_prev,
            next=data_point_next,
        ))
        id_map[(_DESTINATION, data_point_next.id)] = data_point_next

    for id in missing_in_destination:
        original = id_map[(_SOURCE, id)]
        new_data_point = original.copy()
        new_data_point.id = next_id()
        modifications.append(DataPointAction(next=new_data_point))
        index_data_point(_DESTINATION, new_data_point)

    for id in additional_in_destination:
        original = id_map[(_DESTINATION, id)]
        modifications.append(DataPointAction(prev=original))

    destination.add_changes(modifications)
