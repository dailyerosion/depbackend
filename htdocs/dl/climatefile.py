#!/usr/bin/env python
"""Service providing a WEPP climate file."""
import os
import cgi

from pyiem.dep import get_cli_fname
from pyiem.util import ssw


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


def main():
    """Go Main Go."""
    form = cgi.FieldStorage()
    try:
        lat = float(form.getfirst("lat"))
        lon = float(form.getfirst("lon"))
    except (ValueError, TypeError):
        ssw("Content-type: text/plain\n\n")
        ssw("API FAIL!")
        return
    fn = spiral(lon, lat)
    if fn is None:
        ssw("Content-type: text/plain\n\n")
        ssw("API FAIL!")
        return

    ssw("Content-type: application/octet-stream\n")
    ssw("Content-Disposition: attachment; filename=%s\n\n" % (fn.split("/")[-1],))
    ssw(open(fn).read())


if __name__ == "__main__":
    main()
