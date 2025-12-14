from typing import Optional, Union

import grpc


def range_end_for_key(maybe_bytestring: Union[str, bytes]) -> bytes:
    """
    Create a range end key from the provided key. Using the key + the returned range_end key in
    get, delete or transaction compare conditions will result in selection of all keys with the `key` prefix.

    :param maybe_bytestring: key
    :return: another key, byte representation
    """
    key = bytearray(to_bytes(maybe_bytestring))

    # the happy path is that the last byte is not 0xFF; in that
    # case just bump that and go on
    if key[-1] != 0xFF:
        key[-1] = key[-1] + 1

        return bytes(key)

    # but if it is 0xFF, bumping it by one will not work as
    # single byte cannot hold such value.
    #
    # in that case, the range end is obtained by shaving off
    # the last byte. Then the 'new' last byte is increased
    # by one instead. This can possibly happen multiple times.
    for i in range(len(key) - 2, -1, -1):
        if key[i] != 0xFF:
            key[i] += 1

            return bytes(key[: i + 1])

    # this hits if the prefix has all bytes 0xFF; in that case use
    # the explicitly documented range_end of 0x00.
    #
    # see: https://github.com/etcd-io/etcd/blob/d99edb648af6f7ef45b346f97c25def0533e107c/api/etcdserverpb/rpc.proto#L451
    # see local file: proto/etcd3/rpc/rpc.proto:246
    return b"\x00"


def to_bytes(maybe_bytestring: Union[str, bytes]) -> bytes:
    """
    Encode string to bytes.

    Convenience function to do a simple encode('utf-8') if the input is not
    already bytes. Returns the data unmodified if the input is bytes.
    """
    if isinstance(maybe_bytestring, bytes):
        return maybe_bytestring
    else:
        return maybe_bytestring.encode("utf-8")


def get_secure_creds(
    ca_cert: str, cert_key: Optional[str] = None, cert_cert: Optional[str] = None
) -> grpc.ChannelCredentials:
    cert_key_file = None
    cert_cert_file = None

    with open(ca_cert, "rb") as f:
        ca_cert_file = f.read()

    if cert_key is not None:
        with open(cert_key, "rb") as f:
            cert_key_file = f.read()

    if cert_cert is not None:
        with open(cert_cert, "rb") as f:
            cert_cert_file = f.read()

    return grpc.ssl_channel_credentials(ca_cert_file, cert_key_file, cert_cert_file)
