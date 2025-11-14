"""Make website requests."""

import importlib
import os

import pytest
from werkzeug.test import Client, TestResponse


# Load a list of uris provided by a local uris.txt file and test them
# against the SERVICE
def get_uris(extra):
    """Figure out what we need to run for."""
    # Locate the uris.txt file relative to the tests directory
    dirname = os.path.dirname(__file__)
    with open(f"{dirname}/uris{extra}.txt", encoding="ascii") as fh:
        for line in fh.readlines():
            if line.startswith("#") or line.strip() == "":
                continue
            yield line.strip()


def uri2client(uri: str) -> TestResponse:
    """Convert to a client to call."""
    modname = (
        uri.replace("/cgi-bin", "")
        .split("?")[0]
        .rsplit(".", maxsplit=1)[0]
        .replace("/", ".")
    )
    cgi = ""
    if uri.find("?") > 0:
        cgi = uri.split("?")[1]
    mod = importlib.import_module(f"depbackend{modname}")
    return Client(mod.application).get(f"?{cgi}")


@pytest.mark.parametrize("uri", get_uris(""))
def test_uri(uri):
    """Test a URI."""
    res = uri2client(uri)
    # HTTP 400 should be known failures being gracefully handled
    assert res.status_code in [200, 400]


@pytest.mark.parametrize("uri", get_uris("422"))
def test_uri422(uri):
    """Test a URI."""
    res = uri2client(uri)
    assert res.status_code in [422]
