""".. title:: DEP Climate File by Lat/Lon Point

This service provides a download of a climate file for a given latitude and
longitude point.  The service will search for the nearest climate file to the
requested point and provide it for download.

"""

import os
from io import StringIO
from typing import Tuple

import pandas as pd
from pydantic import Field
from pydep.io.wepp import read_cli
from pyiem import iemre
from pyiem.database import get_sqlalchemy_conn
from pyiem.dep import get_cli_fname
from pyiem.util import logger
from pyiem.webutil import CGIModel, ListOrCSVType, iemapp
from sqlalchemy import text

LOG = logger()


class Schema(CGIModel):
    """See how we are called."""

    lat: float = Field(..., description="Latitude of point")
    lon: float = Field(..., description="Longitude of point")
    format: str = Field("wepp", description="Output format, wepp or ntt")
    intensity: ListOrCSVType = Field(
        None, description="Comma delimited list of intensities to compute"
    )


def spiral(lon: float, lat: float) -> Tuple[str, float]:
    """https://stackoverflow.com/questions/398299/looping-in-a-spiral"""
    x = y = 0
    dx = 0
    dy = -1
    # points near the domain edge need to seach a bit further than 0.25deg
    X = 40
    Y = 40
    for _ in range(40**2):
        distance = ((x * 0.01) ** 2 + (y * 0.01) ** 2) ** 0.5
        if (-X / 2 < x <= X / 2) and (-Y / 2 < y <= Y / 2):
            newfn = get_cli_fname(lon + x * 0.01, lat + y * 0.01)
            if os.path.isfile(newfn):
                return newfn, distance
        if x == y or (x < 0 and x == -y) or (x > 0 and x == 1 - y):
            dx, dy = -dy, dx
        x, y = x + dx, y + dy
    return None, None


def log_request(environ: dict, fn: str, distance: float):
    """Log this request"""
    with get_sqlalchemy_conn("idep") as conn:
        conn.execute(
            text(
                "INSERT into clifile_requests(client_addr, geom, "
                "provided_file, distance_degrees) VALUES "
                "(:addr, ST_Point(:lon, :lat, 4326), :fn, :dist)",
            ),
            {
                "lon": environ["lon"],
                "lat": environ["lat"],
                "fn": fn,
                "dist": distance,
                "addr": environ.get("REMOTE_ADDR"),
            },
        )
        conn.commit()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Go Main Go."""
    fmt = environ["format"]
    lon = environ["lon"]
    lat = environ["lat"]
    dom = iemre.DOMAINS[""]
    if (
        lon < dom["west"]
        or lon > dom["east"]
        or lat < dom["south"]
        or lat > dom["north"]
    ):
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        errmsg = (
            f"Requested point outside of bounds {dom['west']},{dom['south']} "
            f"{dom['east']},{dom['north']}!"
        )
        return [errmsg.encode("ascii")]
    fn, distance = spiral(lon, lat)
    if fn is None:
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        return [b"Failed to locate a climate file in vicinity of your point."]
    if fmt == "wepp":
        # Log this request
        try:
            log_request(environ, fn, distance)
        except Exception as exp:
            LOG.exception(exp)

    dlfn = os.path.basename(fn)
    if fmt == "ntt":
        dlfn = dlfn[:-4].replace(".", "_") + ".wth"
    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={dlfn}"),
    ]
    start_response("200 OK", headers)
    if environ["intensity"]:
        levels = [int(x) for x in environ["intensity"]]
        df = read_cli(fn, compute_intensity_over=levels)
        df.index.name = "date"
        df = df.loc[: pd.Timestamp("now")]
        df = df[df["pcpn"] > 0]
        sio = StringIO()
        cols = [
            "pcpn",
        ] + [f"i{x}_mm" for x in levels]
        df[cols].to_csv(sio, float_format="%.2f")
        return sio.getvalue()

    if fmt == "wepp":
        with open(fn, "rb") as fh:
            payload = fh.read()
    elif fmt == "ntt":
        df = read_cli(fn)
        payload = StringIO()
        # Convert langleys to MJ
        df["rad"] = df["rad"] * 0.04184
        for dt, row in df.iterrows():
            payload.write(
                f"  {dt.strftime('%Y %-2m %-2d')}  {row['rad']:3.0f}"
                f"{row['tmax']:6.1f} {row['tmin']:6.1f} {row['pcpn']:6.2f}"
                "\r\n"
            )
        payload = payload.getvalue()
    return payload
