"""search for HUC12 by name."""
import json

from paste.request import parse_formvars
from pyiem.util import get_dbconn


def search(q):
    """Search for q"""
    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()
    d = dict(results=[])
    cursor.execute(
        """SELECT huc_12, hu_12_name from huc12
    WHERE hu_12_name ~* %s and scenario = 0 LIMIT 10""",
        (q,),
    )
    for row in cursor:
        d["results"].append(dict(huc_12=row[0], name=row[1]))

    return d


def application(environ, start_response):
    """DO Something"""
    form = parse_formvars(environ)
    q = form.get("q", "")
    headers = [("Content-type", "application/json")]
    start_response("200 OK", headers)

    return [json.dumps(search(q)).encode("ascii")]
