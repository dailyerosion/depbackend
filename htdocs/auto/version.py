"""Simple JSON of DEP metadata versioning."""
from io import StringIO

import pandas as pd
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn
from sqlalchemy import text


def gen(scenario):
    """Make the map"""
    with get_sqlalchemy_conn("idep") as conn:
        # Check that we have data for this date!
        df = pd.read_sql(
            text(
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


def application(environ, start_response):
    """Do something fun"""
    form = parse_formvars(environ)
    scenario = int(form.get("scenario", 0))
    start_response("200 OK", [("Content-type", "application/json")])
    return [gen(scenario).encode("utf-8")]
