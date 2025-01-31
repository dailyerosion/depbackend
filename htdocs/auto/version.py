"""Simple JSON of DEP metadata versioning."""

from io import StringIO

import pandas as pd
from pydantic import Field
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.webutil import CGIModel, iemapp


class Schema(CGIModel):
    """See how we are called."""

    scenario: int = Field(
        default=0,
        description="Scenario ID to generate metadata for",
    )


def gen(scenario):
    """Make the map"""
    with get_sqlalchemy_conn("idep") as conn:
        # Check that we have data for this date!
        df = pd.read_sql(
            sql_helper(
                """
            select d.* from scenarios s, dep_version d where s.id = :scenario
            and s.dep_version_label = d.label
        """
            ),
            conn,
            params={
                "scenario": scenario,
            },
        )
    sio = StringIO()
    df.iloc[0].to_json(sio)
    return sio.getvalue()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do something fun"""
    start_response("200 OK", [("Content-type", "application/json")])
    return gen(environ["scenario"])
