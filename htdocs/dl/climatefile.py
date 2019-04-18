#!/usr/bin/env python
"""Service providing a WEPP climate file."""
import os
import cgi

from pyiem.dep import get_cli_fname
from pyiem.util import ssw


def spiral(lon, lat):
    """https://stackoverflow.com/questions/398299/looping-in-a-spiral"""
    N = 26
    x, y = 0, 0
    dx, dy = 0, -1

    for _ in range(N * N):
        if abs(x) == abs(y) and [dx, dy] != [1, 0] or x > 0 and y == 1 - x:
            dx, dy = -dy, dx            # corner, change direction

        if abs(x) > N / 2 or abs(y) > N / 2:  # non-square
            dx, dy = -dy, dx                  # change direction
            x, y = -y + dx, x + dy            # jump

        newfn = get_cli_fname(lon + x * 0.01, lat + y * 0.01)
        if os.path.isfile(newfn):
            return newfn
        x, y = x + dx, y + dy
    return None


def main():
    """Go Main Go."""
    form = cgi.FieldStorage()
    try:
        lat = float(form.getfirst('lat'))
        lon = float(form.getfirst('lon'))
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
    ssw("Content-Disposition: attachment; filename=%s\n\n" % (
        fn.split("/")[-1],))
    ssw(open(fn).read())


if __name__ == '__main__':
    main()
