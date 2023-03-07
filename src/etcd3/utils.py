from typing import Optional, Union

import grpc


def range_end_for_key(maybe_bytestring: Union[str, bytes]) -> bytes:
    """
    Create a range end key from the provided key. Using the key + the returned range_end key in
    get, delete or transaction compare conditions will result in selection of all keys with the `key` prefix.

    :param maybe_bytestring: key
    :return: another key, byte representation
    """
    return increment_last_byte(to_bytes(maybe_bytestring))


def increment_last_byte(byte_string: bytes) -> bytes:
    s = bytearray(byte_string)
    s[-1] = s[-1] + 1
    return bytes(s)


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
