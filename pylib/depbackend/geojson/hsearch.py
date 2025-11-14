""".. title:: DEP HUC12 Search by Name or ID

Returns at most 10 results for a fuzzy name search or based on the HUC12 ID
provided.  Sadly, this service does not actually emit GeoJSON.

Changelog
---------

- 14 Nov 2025: The search now checks against the HUC12 ID.

"""

import json

from pydantic import Field
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.webutil import CGIModel, iemapp


class Schema(CGIModel):
    """See how we are called."""

    q: str = Field(..., description="Search query")


def search(q):
    """Search for q"""
    d = dict(results=[])
    with get_sqlalchemy_conn("idep") as conn:
        res = conn.execute(
            sql_helper(
                """SELECT huc_12, name from huc12
                WHERE (name ~* :name or strpos(huc_12, :name) = 1)
                and scenario = 0 LIMIT 10"""
            ),
            {"name": q},
        )
        for row in res:
            d["results"].append(dict(huc_12=row[0], name=row[1]))

    return d


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """DO Something"""
    headers = [("Content-type", "application/json")]
    start_response("200 OK", headers)

    return json.dumps(search(environ["q"]))
