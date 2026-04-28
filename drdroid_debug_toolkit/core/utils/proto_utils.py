import re
from datetime import datetime, timezone
from typing import Dict

from django.http import JsonResponse
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict, ParseDict
from google.protobuf.message import Message, Message as ProtoMessage
from google.protobuf.wrappers_pb2 import BoolValue, UInt32Value

from core.protos.base_pb2 import TimeRange, Page
from core.protos.connectors.api_pb2 import Meta

# Field names that represent uint64 Unix timestamps (seconds).
# Agents may supply ISO 8601 strings; we normalise them here so ParseDict succeeds.
_TIMESTAMP_FIELD_NAMES = frozenset({"time_geq", "time_lt", "time_leq"})
_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def _coerce_timestamps(obj):
    """Recursively walk *obj* and replace ISO 8601 strings in known timestamp
    fields with their Unix-epoch-seconds integer equivalents."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            if k in _TIMESTAMP_FIELD_NAMES and isinstance(v, str) and _ISO8601_RE.match(v):
                try:
                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                    result[k] = int(dt.timestamp())
                except (ValueError, OverflowError):
                    result[k] = v
            else:
                result[k] = _coerce_timestamps(v)
        return result
    if isinstance(obj, list):
        return [_coerce_timestamps(item) for item in obj]
    return obj


class ProtoException(ValueError):
    pass

class ProtoJsonResponse(JsonResponse):
    def __init__(
            self,
            data,
            **kwargs,
    ):
        if not isinstance(data, ProtoMessage):
            raise TypeError(
                f"data must be an instance of google.protobuf.message.Message, not {type(data)}"
            )

        json_data: Dict = proto_to_dict(data)
        super().__init__(json_data, **kwargs)


class ProtoJsonResponse(JsonResponse):
    def __init__(
            self,
            data,
            **kwargs,
    ):
        if not isinstance(data, ProtoMessage):
            raise TypeError(
                f"data must be an instance of google.protobuf.message.Message, not {type(data)}"
            )

        json_data: Dict = proto_to_dict(data)
        super().__init__(json_data, **kwargs)

def proto_to_json(obj: Message, use_snake_case: bool = True) -> str:
    if not obj:
        raise ProtoException('Trying to serialize None obj')
    try:
        text = MessageToJson(obj, preserving_proto_field_name=use_snake_case)
    except Exception as e:
        raise ProtoException(f'Error serializing proto message: {e}')
    return text


def proto_to_dict(obj: Message, use_snake_case: bool = True) -> Dict:
    if not obj:
        raise ProtoException('Trying to serialize None obj')
    try:
        message_dict = MessageToDict(obj, preserving_proto_field_name=use_snake_case)
    except Exception as e:
        raise ProtoException(f'Error converting proto message to dict: {e}')
    return message_dict


def json_to_proto(text: str, proto_clazz, ignore_unknown_fields=True) -> Message:
    if proto_clazz:
        msg = proto_clazz()
    else:
        raise ProtoException('No message class defined')

    try:
        msg = Parse(text, msg, ignore_unknown_fields)
    except Exception as e:
        raise ProtoException(f'Error while parsing text: {e}')
    return msg


def dict_to_proto(d: Dict, proto_clazz, ignore_unknown_fields=True) -> Message:
    if proto_clazz:
        msg = proto_clazz()
    else:
        raise ProtoException('No message class defined')

    try:
        msg = ParseDict(_coerce_timestamps(d), msg, ignore_unknown_fields)
    except Exception as e:
        raise ProtoException(f'Error while parsing text: {e}')
    return msg


def get_meta(tr: TimeRange = TimeRange(), page: Page = Page(), total_count: int = 0,
             show_inactive: BoolValue = BoolValue(value=False)):
    return Meta(time_range=tr, page=page, total_count=UInt32Value(value=total_count), show_inactive=show_inactive)
