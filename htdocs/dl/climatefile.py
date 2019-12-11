"""Service providing a WEPP climate file."""
import os

from paste.request import parse_formvars
from pyiem.dep import get_cli_fname


def spiral(lon, lat):
    """https://stackoverflow.com/questions/398299/looping-in-a-spiral"""
    x = y = 0
    dx = 0
    dy = -1
    # points near the domain edge need to seach a bit further than 0.25deg
    X = 40
    Y = 40
    for _ in range(40 ** 2):
        if (-X / 2 < x <= X / 2) and (-Y / 2 < y <= Y / 2):
            newfn = get_cli_fname(lon + x * 0.01, lat + y * 0.01)
            if os.path.isfile(newfn):
                return newfn
        if x == y or (x < 0 and x == -y) or (x > 0 and x == 1 - y):
            dx, dy = -dy, dx
        x, y = x + dx, y + dy
    return None


def application(environ, start_response):
    """Go Main Go."""
    form = parse_formvars(environ)
    try:
        lat = float(form.get("lat"))
        lon = float(form.get("lon"))
    except (ValueError, TypeError):
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        return [b"API FAIL!"]
    fn = spiral(lon, lat)
    if fn is None:
        headers = [("Content-type", "text/plain")]
        start_response("500 Internal Server Error", headers)
        return [b"API FAIL!"]

    headers = [
        ("Content-type", "application/octet-stream"),
        (
            "Content-Disposition",
            "attachment; filename=%s" % (fn.split("/")[-1],),
        ),
    ]
    start_response("200 OK", headers)
    return [open(fn, "rb").read()]
