""".. title:: WEPS Hourly Wind Files supporting DEP

This service emits an hourly wind file used by the Daily Erosion Project to
model soil erosion by wind via the WEPS model.  The files are updated daily
at about 1 AM central time.  The files will contain fake data from the future
for the current year so to have serially complete data for the current year.

The file will contain data from 2007 to the current date's year.

Changelog
---------

- 2026-06-05: Initial sevice release

Example Requests
----------------

Provide the wind file for a point in Iowa:

https://mesonet-dep.agron.iastate.edu/dl/weps_hourly_wind.py?lat=42.0&lon=-93.0

"""

from pathlib import Path
from typing import Annotated

from pydantic import Field
from pyiem.exceptions import NoDataFound
from pyiem.iemre import get_gid, get_domain
from pyiem.util import logger
from pyiem.webutil import CGIModel, iemapp

LOG = logger()


class Schema(CGIModel):
    """See how we are called."""

    lat: Annotated[
        float,
        Field(description="Latitude of point, degrees North", ge=-90, le=90),
    ]
    lon: Annotated[
        float,
        Field(description="Longitude of point, degrees East", ge=-180, le=180),
    ]


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Go Main Go."""
    query: Schema = environ["_cgimodel_schema"]
    # Ensure that our domain matches the reality
    if (domain := get_domain(query.lon, query.lat)) != "conus":
        raise NoDataFound("Sorry, only CONUS data is supported at the moment")
    gid = get_gid(query.lon, query.lat, domain)
    padded_gid = f"{gid:06.0f}"
    fn = Path(f"/i/0/wind/{padded_gid[:3]}/{padded_gid}.win")
    if not fn.is_file():
        raise NoDataFound(
            f"Sorry, no data found for {query.lon:.2f}E {query.lat:.2f}N, "
            f"iemre gid: {gid}"
        )
    # meh, filename without dashes is probably better, for now.
    lonstr = "W" if query.lon < 0 else "E"
    latstr = "S" if query.lat < 0 else "N"
    dlfn = (
        "weps_hourly_wind_"
        f"{abs(query.lon):.2f}{lonstr}_{abs(query.lat):.2f}{latstr}.win"
    )

    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={dlfn}"),
    ]
    start_response("200 OK", headers)
    with open(fn, "rb") as fh:
        payload = fh.read()
    return payload
