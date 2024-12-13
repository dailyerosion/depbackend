""".. title:: DEP HUC12 Summary for OFETool Usage

This service provides a summary of DEP outputs by OFE for a HUC12.

"""

import os

from pydantic import Field
from pyiem.util import logger
from pyiem.webutil import CGIModel, iemapp

LOG = logger()


class Schema(CGIModel):
    """See how we are called."""

    huc12: str = Field(
        ..., description="HUC12 identifier", pattern=r"^\d{12}$"
    )
    summarize: bool = Field(
        default=True,
        description="Should the output be summarized or raw?",
    )


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Go Main Go."""
    huc12: str = environ["huc12"]
    prefix = "tool" if environ["summarize"] else "results"
    fn = f"/i/0/ofe/{huc12[:8]}/{huc12[8:]}/ofe{prefix}_{huc12}.csv"
    if not os.path.isfile(fn):
        headers = [("Content-type", "text/plain")]
        start_response("404 Not Found", headers)
        return [b"ERROR: No data found for given HUC12"]
    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={fn.split('/')[-1]}"),
    ]
    start_response("200 OK", headers)
    with open(fn, "rb") as fh:
        return fh.read()
