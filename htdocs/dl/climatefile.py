"""Service providing a WEPP climate file."""

import os
from io import StringIO

import pandas as pd
from pydantic import Field
from pydep.io.wepp import read_cli
from pyiem.dep import get_cli_fname
from pyiem.iemre import EAST, NORTH, SOUTH, WEST
from pyiem.webutil import CGIModel, ListOrCSVType, iemapp


class Schema(CGIModel):
    """See how we are called."""

    lat: float = Field(..., description="Latitude of point")
    lon: float = Field(..., description="Longitude of point")
    format: str = Field("wepp", description="Output format, wepp or ntt")
    intensity: ListOrCSVType = Field(
        None, description="Comma delimited list of intensities to compute"
    )


def spiral(lon, lat):
    """https://stackoverflow.com/questions/398299/looping-in-a-spiral"""
    x = y = 0
    dx = 0
    dy = -1
    # points near the domain edge need to seach a bit further than 0.25deg
    X = 40
    Y = 40
    for _ in range(40**2):
        if (-X / 2 < x <= X / 2) and (-Y / 2 < y <= Y / 2):
            newfn = get_cli_fname(lon + x * 0.01, lat + y * 0.01)
            if os.path.isfile(newfn):
                return newfn
        if x == y or (x < 0 and x == -y) or (x > 0 and x == 1 - y):
            dx, dy = -dy, dx
        x, y = x + dx, y + dy
    return None


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Go Main Go."""
    fmt = environ["format"]
    lon = environ["lon"]
    lat = environ["lat"]
    # 23 Jun 2022 restrict domain to decent data bounds
    if lon < WEST or lon > EAST or lat < SOUTH or lat > NORTH:
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        errmsg = (
            f"Requested point outside of bounds {WEST},{SOUTH} {EAST},{NORTH}!"
        )
        return [errmsg.encode("ascii")]
    fn = spiral(lon, lat)
    if fn is None:
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        return [b"Failed to locate a climate file in vicinity of your point."]

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
