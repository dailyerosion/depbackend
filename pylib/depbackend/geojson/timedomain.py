"""Return the time domain that we have DEP data for, given this scenario"""

import json

from pydantic import Field
from pyiem.database import sql_helper, with_sqlalchemy_conn
from pyiem.util import utc
from pyiem.webutil import CGIModel, iemapp
from sqlalchemy.engine import Connection

ISO = "%Y-%m-%dT%H:%M:%SZ"


class Schema(CGIModel):
    """See how we are called."""

    scenario: int = Field(0, description="Scenario to query for")


@with_sqlalchemy_conn("idep")
def get_time(scenario: int, conn: Connection = None) -> dict:
    """Search for q"""
    d = dict()
    d["server_time"] = utc().strftime(ISO)
    d["first_date"] = None
    d["last_date"] = None
    d["scenario"] = scenario
    key = f"last_date_{scenario}"
    res = conn.execute(
        sql_helper("SELECT value from properties WHERE key = :key"),
        {"key": key},
    )
    if res.rowcount == 1:
        row = res.fetchone()
        d["first_date"] = "2007-01-01"
        d["last_date"] = row[0]
    return d


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """DO Something"""
    headers = [("Content-type", "application/json")]
    start_response("200 OK", headers)
    return json.dumps(get_time(environ["scenario"]))
