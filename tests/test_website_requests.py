"""Make website requests."""

import os

import pytest
import requests

SERVICE = "http://depbackend.local"


# Load a list of uris provided by a local uris.txt file and test them
# against the SERVICE
def get_uris():
    """Figure out what we need to run for."""
    # Locate the uris.txt file relative to the tests directory
    dirname = os.path.dirname(__file__)
    with open(f"{dirname}/uris.txt", encoding="ascii") as fh:
        for line in fh.readlines():
            if line.startswith("#"):
                continue
            yield line.strip()


@pytest.mark.parametrize("uri", get_uris())
def test_uri(uri):
    """Test a URI."""
    res = requests.get(f"{SERVICE}{uri}", timeout=60)
    # HTTP 400 should be known failures being gracefully handled
    assert res.status_code in [200, 400]
