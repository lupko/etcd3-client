# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: etcd3/rpc/version.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b"\n\x17\x65tcd3/rpc/version.proto\x12\tversionpb\x1a google/protobuf/descriptor.proto:N\n\x10\x65tcd_version_msg\x12\x1f.google.protobuf.MessageOptions\x18\xd0\x86\x03 \x01(\tR\x0e\x65tcdVersionMsg\x88\x01\x01:P\n\x12\x65tcd_version_field\x12\x1d.google.protobuf.FieldOptions\x18\xd1\x86\x03 \x01(\tR\x10\x65tcdVersionField\x88\x01\x01:M\n\x11\x65tcd_version_enum\x12\x1c.google.protobuf.EnumOptions\x18\xd2\x86\x03 \x01(\tR\x0f\x65tcdVersionEnum\x88\x01\x01:]\n\x17\x65tcd_version_enum_value\x12!.google.protobuf.EnumValueOptions\x18\xd3\x86\x03 \x01(\tR\x14\x65tcdVersionEnumValue\x88\x01\x01\x42\x61\n\rcom.versionpbB\x0cVersionProtoP\x01\xa2\x02\x03VXX\xaa\x02\tVersionpb\xca\x02\tVersionpb\xe2\x02\x15Versionpb\\GPBMetadata\xea\x02\tVersionpbb\x06proto3"
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "etcd3.rpc.version_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    google_dot_protobuf_dot_descriptor__pb2.MessageOptions.RegisterExtension(
        etcd_version_msg
    )
    google_dot_protobuf_dot_descriptor__pb2.FieldOptions.RegisterExtension(
        etcd_version_field
    )
    google_dot_protobuf_dot_descriptor__pb2.EnumOptions.RegisterExtension(
        etcd_version_enum
    )
    google_dot_protobuf_dot_descriptor__pb2.EnumValueOptions.RegisterExtension(
        etcd_version_enum_value
    )

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\rcom.versionpbB\014VersionProtoP\001\242\002\003VXX\252\002\tVersionpb\312\002\tVersionpb\342\002\025Versionpb\\GPBMetadata\352\002\tVersionpb"
# @@protoc_insertion_point(module_scope)
