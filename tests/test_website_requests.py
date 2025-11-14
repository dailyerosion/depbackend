"""Make website requests."""

import importlib
import os

import pytest
from werkzeug.test import Client


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
            modname = (
                line.replace("/cgi-bin", "")
                .split("?")[0]
                .rsplit(".", maxsplit=1)[0]
                .replace("/", ".")
            )
            cgi = ""
            if line.find("?") > 0:
                cgi = line.split("?")[1]
            mod = importlib.import_module(f"depbackend{modname}")
            yield mod.application, f"?{cgi}"


@pytest.mark.parametrize("arg", get_uris(""))
def test_uri(arg):
    """Test a URI."""
    c = Client(arg[0])
    res = c.get(arg[1])
    # HTTP 400 should be known failures being gracefully handled
    assert res.status_code in [200, 400]


@pytest.mark.parametrize("arg", get_uris("422"))
def test_uri422(arg):
    """Test a URI."""
    c = Client(arg[0])
    res = c.get(arg[1])
    assert res.status_code in [422]
