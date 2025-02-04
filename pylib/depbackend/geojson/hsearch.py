"""search for HUC12 by name."""

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
                "SELECT huc_12, name from huc12 "
                "WHERE name ~* :name and scenario = 0 LIMIT 10"
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
