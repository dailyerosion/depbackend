"""HUC12 Summary Info."""

from datetime import datetime
from io import StringIO

import pandas as pd
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn
from sqlalchemy import text


def gen(huc12s, sdate, edate):
    """Make the map"""
    with get_sqlalchemy_conn("idep") as conn:
        # Check that we have data for this date!
        df = pd.read_sql(
            text(
                """
            SELECT huc_12,
            sum(avg_loss) * 4.463 as avg_loss_ton_acre,
            sum(avg_delivery) * 4.463 as avg_delivery_ton_acre,
            sum(qc_precip) / 25.4 as rain_inch
            from results_by_huc12
            WHERE huc_12 in :h and scenario = 0
            and valid >= :sdate and valid <= :edate
            GROUP by huc_12 ORDER by huc_12
        """
            ),
            conn,
            params={
                "h": tuple(huc12s),
                "sdate": sdate,
                "edate": edate,
            },
        )
    sio = StringIO()
    df.to_csv(sio, index=False, float_format="%.2f")
    return sio.getvalue()


def application(environ, start_response):
    """Do something fun"""
    form = parse_formvars(environ)
    huc12s = [x[:12] for x in form.getall("huc12")][:64]
    sdate = datetime.strptime(form.get("sdate", "2022-01-01"), "%Y-%m-%d")
    edate = datetime.strptime(form.get("edate", "2022-07-01"), "%Y-%m-%d")

    start_response("200 OK", [("Content-type", "text/plain")])
    return [gen(huc12s, sdate, edate).encode("utf-8")]
