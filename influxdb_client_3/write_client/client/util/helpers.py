"""Functions to share utility across client classes."""
from typing import Optional, Union

from cryptography.hazmat.primitives import serialization


def _is_id(value):
    """
    Check if the value is valid InfluxDB ID.

    :param value: to check
    :return: True if provided parameter is valid InfluxDB ID.
    """
    if value and len(value) == 16:
        try:
            int(value, 16)
            return True
        except ValueError:
            return False
    return False


def get_org_query_param(org, client, required_id=False):
    """
    Get required type of Org query parameter.

    :param str, Organization org: value provided as a parameter into API (optional)
    :param InfluxDBClient client: with default value for Org parameter
    :param bool required_id: true if the query param has to be a ID
    :return: request type of org query parameter or None
    """
    _org = client.org if org is None else org
    if 'Organization' in type(_org).__name__:
        _org = _org.id
    if required_id and _org and not _is_id(_org):
        try:
            organizations = client.organizations_api().find_organizations(org=_org)
            if len(organizations) < 1:
                from influxdb_client_3.exceptions import InfluxDBRestClientException
                message = f"The client cannot find organization with name: '{_org}' " \
                          "to determine their ID. Are you using token with sufficient permission?"
                raise InfluxDBRestClientException(response=None, message=message)
            return organizations[0].id
        except InfluxDBRestClientException as e:
            if e.status == 404:
                from influxdb_client_3.exceptions import InfluxDBRestClientException
                message = f"The client cannot find organization with name: '{_org}' " \
                          "to determine their ID."
                raise InfluxDBRestClientException(response=None, message=message)
            raise e

    return _org


def read_certs(path: str, password: Optional[Union[str, bytes]] = None) -> bytes:
    with open(path, "rb") as certs_file:
        data = certs_file.read()
        if password is not None:
            password_bytes = password.encode() if isinstance(password, str) else password
            private_key_obj = serialization.load_pem_private_key(
                data,
                password=password_bytes,
            )
            data = private_key_obj.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )

    # Normalize PEM line endings so behavior is stable across platforms.
    return data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
