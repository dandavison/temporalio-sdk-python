"""Converting a record's payload to a value."""

from __future__ import annotations

from typing import Any

import temporalio.converter
from temporalio.streams._record import Record


async def decode(
    record: Record,
    type_hint: type | None = None,
    data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
) -> Any:
    """Decode and convert a data record's payload."""
    if record.payload is None:
        raise ValueError("record has no payload")
    [value] = await data_converter.decode(
        [record.payload], [type_hint] if type_hint else None
    )
    return value
