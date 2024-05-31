"""Return the time domain that we have DEP data for, given this scenario"""

import datetime
import json

from pydantic import Field
from pyiem.database import get_dbconn
from pyiem.webutil import CGIModel, iemapp

ISO = "%Y-%m-%dT%H:%M:%SZ"


class Schema(CGIModel):
    """See how we are called."""

    scenario: int = Field(0, description="Scenario to query for")


def get_time(scenario):
    """Search for q"""
    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()
    d = dict()
    d["server_time"] = datetime.datetime.utcnow().strftime(ISO)
    d["first_date"] = None
    d["last_date"] = None
    d["scenario"] = scenario
    key = f"last_date_{scenario}"
    cursor.execute(
        "SELECT value from properties WHERE key = %s",
        (key,),
    )
    if cursor.rowcount == 1:
        row = cursor.fetchone()
        d["first_date"] = datetime.date(2007, 1, 1).strftime(ISO)
        d["last_date"] = datetime.datetime.strptime(
            row[0], "%Y-%m-%d"
        ).strftime(ISO)
    cursor.close()
    pgconn.close()
    return d


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """DO Something"""
    headers = [("Content-type", "application/json")]
    start_response("200 OK", headers)
    return json.dumps(get_time(environ["scenario"]))
