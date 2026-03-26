from google.protobuf import wrappers_pb2 as _wrappers_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from core.protos import base_pb2 as _base_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class McpAssetModel(_message.Message):
    __slots__ = ("id", "connector_type", "type", "last_updated", "name", "metadata")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATED_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    id: _wrappers_pb2.UInt64Value
    connector_type: _base_pb2.Source
    type: _base_pb2.SourceModelType
    last_updated: int
    name: _wrappers_pb2.StringValue
    metadata: _struct_pb2.Struct
    def __init__(self, id: _Optional[_Union[_wrappers_pb2.UInt64Value, _Mapping]] = ..., connector_type: _Optional[_Union[_base_pb2.Source, str]] = ..., type: _Optional[_Union[_base_pb2.SourceModelType, str]] = ..., last_updated: _Optional[int] = ..., name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class McpAssets(_message.Message):
    __slots__ = ("assets",)
    ASSETS_FIELD_NUMBER: _ClassVar[int]
    assets: _containers.RepeatedCompositeFieldContainer[McpAssetModel]
    def __init__(self, assets: _Optional[_Iterable[_Union[McpAssetModel, _Mapping]]] = ...) -> None: ...

class McpAssetOptions(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
