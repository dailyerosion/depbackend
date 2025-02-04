"""HUC12 Summary Info."""

from datetime import date
from io import StringIO

import pandas as pd
from pydantic import Field
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.webutil import CGIModel, ListOrCSVType, iemapp


class Schema(CGIModel):
    """See how we are called."""

    huc12: ListOrCSVType = Field(
        default=["070600040601"],
        description="Comma delimited list of HUC12s to summarize",
    )
    sdate: date = Field(
        default=date(2022, 1, 1),
        description="Start date to summarize",
    )
    edate: date = Field(
        default=date(2022, 7, 1),
        description="End date to summarize",
    )


def gen(huc12s, sdate, edate):
    """Make the map"""
    with get_sqlalchemy_conn("idep") as conn:
        # Check that we have data for this date!
        df = pd.read_sql(
            sql_helper(
                """
            SELECT huc_12,
            sum(avg_loss) * 4.463 as avg_loss_ton_acre,
            sum(avg_delivery) * 4.463 as avg_delivery_ton_acre,
            sum(qc_precip) / 25.4 as rain_inch
            from results_by_huc12
            WHERE huc_12 = Any(:h) and scenario = 0
            and valid >= :sdate and valid <= :edate
            GROUP by huc_12 ORDER by huc_12
        """
            ),
            conn,
            params={
                "h": huc12s,
                "sdate": sdate,
                "edate": edate,
            },
        )
    sio = StringIO()
    df.to_csv(sio, index=False, float_format="%.2f")
    return sio.getvalue()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do something fun"""
    huc12s = [x[:12] for x in environ["huc12"]][:64]

    start_response("200 OK", [("Content-type", "text/plain")])
    return [gen(huc12s, environ["sdate"], environ["edate"]).encode("utf-8")]
