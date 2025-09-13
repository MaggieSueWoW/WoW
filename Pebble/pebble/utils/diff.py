from typing import Iterable, Dict, Any, Tuple


def keyed(iterable: Iterable[Dict[str, Any]], key_fields: Tuple[str, ...]):
    for row in iterable:
        key = tuple(row[k] for k in key_fields)
        yield key, row
