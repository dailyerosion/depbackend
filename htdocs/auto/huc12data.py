"""JSON service for HUC12 data"""
import datetime

from pymemcache.client import Client

# needed for Decimal formatting to work
import simplejson as json
import pandas as pd
from paste.request import parse_formvars
from pyiem.dep import RAMPS
from pyiem.util import get_sqlalchemy_conn, utc


def do(ts, ts2):
    """Do work"""
    with get_sqlalchemy_conn("idep") as conn:
        df = pd.read_sql(
            """
        with data as (
            SELECT huc_12,
            sum(coalesce(avg_loss, 0)) * 4.463 as avg_loss,
            sum(coalesce(avg_delivery, 0)) * 4.463 as avg_delivery,
            sum(coalesce(qc_precip, 0)) / 25.4 as qc_precip,
            sum(coalesce(avg_runoff, 0)) / 25.4 as avg_runoff
            from results_by_huc12 WHERE valid >= %s and valid <= %s
            and scenario = 0 GROUP by huc_12)

        SELECT h.huc_12,
        coalesce(round(d.avg_loss::numeric, 2), 0) as avg_loss,
        coalesce(round(d.qc_precip::numeric, 2), 0) as qc_precip,
        coalesce(round(d.avg_delivery::numeric, 2), 0) as avg_delivery,
        coalesce(round(d.avg_runoff::numeric, 2), 0) as avg_runoff
        from huc12 h LEFT JOIN data d ON (h.huc_12 = d.huc_12) WHERE
        h.scenario = 0
        """,
            conn,
            params=(ts, ts2),
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


def application(environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/json")]
    start_response("200 OK", headers)
    form = parse_formvars(environ)
    ts = datetime.datetime.strptime(form.get("sdate"), "%Y%m%d")
    ts2 = datetime.datetime.strptime(form.get("edate"), "%Y%m%d")

    mckey = f"/json/huc12data/{ts:%Y%m%d}_{ts2:%Y%m%d}/v2"
    mc = Client(["iem-memcached", 11211])
    res = mc.get(mckey)
    if res is None:
        res = do(ts, ts2)
        mc.set(mckey, res, 3600)
    else:
        res = res.decode("utf-8")
    mc.close()
    return [res.encode("ascii")]
