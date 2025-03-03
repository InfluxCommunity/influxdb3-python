import os
import time

import pyarrow

from config import Config
from influxdb_client_3 import InfluxDBClient3

bad_cert = """-----BEGIN CERTIFICATE-----
MIIFDTCCAvWgAwIBAgIUYzpfisy9xLrhiZd+D9vOdzC3+iswDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLdGVzdGhvc3QuaW8wHhcNMjUwMjI4MTM1NTMyWhcNMzUw
MjI2MTM1NTMyWjAWMRQwEgYDVQQDDAt0ZXN0aG9zdC5pbzCCAiIwDQYJKoZIhvcN
AQEBBQADggIPADCCAgoCggIBAN1lwqXYP8UMvjb56SpUEj2OpoEDRfLeWrEiHkOl
xoymvJGaXZNEpDXo2TTdysCoYWEjz9IY6GlqSo2Yssf5BZkQwMOw7MdyRwCigzrh
OAKbyCfsvEgfNFrXEdSDpaxW++5SToeErudYXc+sBfnI1NB4W3GBGqqIvx8fqaB3
1EU9ql2sKKxI0oYIQD/If9rQEyLFKeWdD8iT6YST1Vugkvd34NPmaqV5+pjdSb4z
a8olavwUoslqFUeILqIq+WZZbOlgCcJYKcBAmELRnsxGaABRtMwMZx+0D+oKo4Kl
QQtOcER+RHkBHyYFghZIBnzudfbP9NadknOz3AilJbJolXfXJqeQhRD8Ob49kkhe
OwjAppHnaZGWjYZMLIfnwwXBwkS7bSwF16Wot83cpL46Xvg6xcl12An4JaoF798Q
cXyYrWCgvbqjVR7694gxqLGzk138AKTDSbER1h1rfqCqkk7soE0oWCs7jiCk2XvD
49qVfHtd50KYJ4/yP1XL0PmLL0Hw1kvOxLVkFENc1zkoYXJRt2Ec6j9dajmGlsFn
0bLLap6UIlIGQFuvcLf4bvsIi9FICy2jBjaIdM4UAWbReG+52+180HEleAwi5bAN
HY61WVXc4X+N0E2y8HWc1QaRioU7R4XZ5HXKs7OTWkKFZUU2JDFHAKdiiAU78qLU
7GApAgMBAAGjUzBRMB0GA1UdDgQWBBT2vPFo0mzh9ls4xJUiAgSK+B5LpTAfBgNV
HSMEGDAWgBT2vPFo0mzh9ls4xJUiAgSK+B5LpTAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4ICAQC4TJNPx476qhiMi8anISv9lo9cnLju+qNhcz7wupBH
3Go6bVQ7TCbSt2QpAyY64mdnRqHsXeGvZXCnabOpeKRDeAPBtRjc6yNKuXybqFtn
W3PZEs/OYc659TUA+MoBzSXYStN9yiiYXyVFqVn+Rw6kM9tKh0GgAU7f5P+8IGuR
gXJbCjkbdJO7JUiVGEEmkjUHyqFxMHaZ8V6uazs52qIFyt7OYQTeV9HdoW8D9vAt
GfzYwzRDzbsZeIJqqDzLe7NOyxEyqZHCbtNpGcOyaLOl7ZBS52WsqaUZtL+9PjqD
2TWj4WUFkOWQpTvWKHqM6//Buv4GjnTBShQKm+h+rxcGkdRMF6/sKwxPbr39P3RJ
TMfJA3u5UuowT44VaA2jkQzqIbxH9+3EA+0qPbqPJchOSr0pHSncqvR9FYcr7ayN
b6UDFnjeliyEqqksUO0arbvaO9FfB0kH8lU1NOKaQNO++Xj69GZMC6s721cNdad0
qqcdtyXWeOBBchguYDrSUIgLnUTHEwwzOmcNQ36hO5eX282BJy3ZLT3JU6MJopjz
vkbDDAxSrpZMcaoAWSrxgJAETeYiO4YbfORIzPkwdUkEIr6XY02Pi7MdkDGQ5hiB
TavA8+oXRa4b9BR3bCWcg8S/t4uOTTLkeTcQbONPh5A5IRySLCU+CwqB+/+VlO8X
Aw==
-----END CERTIFICATE-----"""


def write_cert(cert, file_name):
    f = open(file_name, "w")
    f.write(cert)
    f.close()


def remove_cert(file_name):
    os.remove(file_name)


def print_results(results: list):
    print("%-6s%-6s%-6s%-24s" % ("id", "speed", "ticks", "time"))
    for result in results:
        print("%-6s%-6.2f%-6i%-24s" % (result['id'], result['speed'], result['ticks'], result['time']))


def main() -> None:
    print("Main")
    temp_cert_file = "temp_cert.pem"
    conf = Config()

    write_and_query_with_explicit_sys_cert(conf)

    write_cert(bad_cert, temp_cert_file)
    query_with_verify_ssl_off(conf, temp_cert_file)
    remove_cert(temp_cert_file)


def write_and_query_with_explicit_sys_cert(conf):
    print("\nwrite and query with typical linux system cert\n")
    with InfluxDBClient3(token=conf.token,
                         host=conf.host,
                         org=conf.org,
                         database=conf.database,
                         ssl_ca_cert="/etc/ssl/certs/ca-certificates.crt",
                         verify_ssl=True) as _client:
        now = time.time_ns()
        lp = f"escooter,id=zx80 speed=3.14,ticks=42i {now - (10 * 1_000_000_000)}"
        _client.write(lp)

        query = "SELECT * FROM \"escooter\" ORDER BY time DESC"
        reader: pyarrow.Table = _client.query(query, mode="")
        print_results(reader.to_pylist())


def query_with_verify_ssl_off(conf, cert):
    print("\nquerying with verify_ssl off\n")

    # Note that the passed root cert above is bad
    # Switch verify_ssl to True to throw SSL_ERROR_SSL
    with InfluxDBClient3(token=conf.token,
                         host=conf.host,
                         org=conf.org,
                         database=conf.database,
                         ssl_ca_cert=cert,
                         verify_ssl=False) as _client:

        query = "SELECT * FROM \"escooter\"  ORDER BY time DESC"
        reader: pyarrow.Table = _client.query(query, mode="")
        print_results(reader.to_pylist())


if __name__ == "__main__":
    main()
