"""Simple JSON of DEP metadata versioning."""

from io import StringIO

import pandas as pd
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.exceptions import NoDataFound
from pyiem.webutil import CGIModel, iemapp

from depbackend.fields import SCENARIO_FIELD


class Schema(CGIModel):
    """See how we are called."""

    scenario: SCENARIO_FIELD = 0


def gen(scenario):
    """Make the map"""
    with get_sqlalchemy_conn("dep") as conn:
        # Check that we have data for this date!
        df = pd.read_sql(
            sql_helper(
                """
            select d.* from scenario s, dep_version d
            where s.scenario_id = :scenario
            and s.dep_version_label = d.label
        """
            ),
            conn,
            params={
                "scenario": scenario,
            },
        )
    if df.empty:
        raise NoDataFound("No data found for scenario")
    sio = StringIO()
    df.iloc[0].to_json(sio)
    return sio.getvalue()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do something fun"""
    payload = gen(environ["scenario"])
    start_response("200 OK", [("Content-type", "application/json")])
    return payload
