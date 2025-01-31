"""JSON service for HUC12 data"""

from datetime import date

import pandas as pd
import simplejson as json
from pydantic import Field
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.dep import RAMPS
from pyiem.util import utc
from pyiem.webutil import CGIModel, iemapp


class Schema(CGIModel):
    """See how we are called."""

    sdate: date = Field(
        default=date(2010, 1, 1),
        description="Start Date",
    )
    edate: date = Field(
        default=date(2010, 1, 1),
        description="End Date",
    )


def do(ts, ts2):
    """Do work"""
    with get_sqlalchemy_conn("idep") as conn:
        df = pd.read_sql(
            sql_helper("""
        with data as (
            SELECT huc_12,
            sum(coalesce(avg_loss, 0)) * 4.463 as avg_loss,
            sum(coalesce(avg_delivery, 0)) * 4.463 as avg_delivery,
            sum(coalesce(qc_precip, 0)) / 25.4 as qc_precip,
            sum(coalesce(avg_runoff, 0)) / 25.4 as avg_runoff
            from results_by_huc12 WHERE valid >= :sdate and valid <= :edate
            and scenario = 0 GROUP by huc_12)

        SELECT h.huc_12,
        coalesce(round(d.avg_loss::numeric, 2), 0) as avg_loss,
        coalesce(round(d.qc_precip::numeric, 2), 0) as qc_precip,
        coalesce(round(d.avg_delivery::numeric, 2), 0) as avg_delivery,
        coalesce(round(d.avg_runoff::numeric, 2), 0) as avg_runoff
        from huc12 h LEFT JOIN data d ON (h.huc_12 = d.huc_12) WHERE
        h.scenario = 0
        """),
            conn,
            params={"sdate": ts, "edate": ts2},
            index_col=None,
        )
    res = {
        "data": df.to_dict(orient="records"),
        "date": ts.strftime("%Y-%m-%d"),
        "date2": None if ts2 is None else ts2.strftime("%Y-%m-%d"),
        "generation_time": utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(df.index),
    }
    myramp = RAMPS["english"][0]
    if ts2 is not None:
        days = (ts2 - ts).days
        myramp = RAMPS["english"][1]
        if days > 31:
            myramp = RAMPS["english"][2]

    res["ramps"] = dict(
        avg_loss=myramp,
        qc_precip=myramp,
        avg_delivery=myramp,
        avg_runoff=myramp,
    )
    res["max_values"] = dict(
        avg_loss=df["avg_loss"].max(),
        qc_precip=df["qc_precip"].max(),
        avg_delivery=df["avg_delivery"].max(),
        avg_runoff=df["avg_runoff"].max(),
    )
    return json.dumps(res)


def get_mckey(environ):
    """Figure out the memcache key"""
    return (
        f"/json/huc12data/{environ['sdate']:%Y%m%d}_{environ['edate']:%Y%m%d}"
    )


@iemapp(help=__doc__, schema=Schema, memcachekey=get_mckey)
def application(environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/json")]
    start_response("200 OK", headers)
    return do(environ["sdate"], environ["edate"])
