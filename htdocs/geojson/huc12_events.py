"""GeoJSON service for HUC12 data"""
import datetime
from io import BytesIO
import json

import pandas as pd
from pymemcache.client import Client
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn

EXL = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def do(huc12, mode, fmt):
    """Do work"""
    utcnow = datetime.datetime.utcnow()
    if mode == "daily":
        with get_sqlalchemy_conn("idep") as conn:
            df = pd.read_sql(
                """
                SELECT valid,
                avg_loss * 4.463 as avg_loss,
                avg_delivery * 4.463 as avg_delivery,
                qc_precip / 25.4 as qc_precip,
                avg_runoff / 25.4 as avg_runoff,
                1 as avg_loss_events,
                1 as avg_delivery_events,
                1 as qc_precip_events,
                1 as avg_runoff_events
                from results_by_huc12 where huc_12 = %s and scenario = 0 ORDER
                by valid ASC
            """,
                conn,
                params=(huc12,),
                index_col=None,
            )
    else:
        with get_sqlalchemy_conn("idep") as conn:
            df = pd.read_sql(
                """
                SELECT extract(year from valid)::int as yr,
                sum(avg_loss) * 4.463 as avg_loss,
                sum(avg_delivery) * 4.463 as avg_delivery,
                sum(qc_precip) / 25.4 as qc_precip,
                sum(avg_runoff) / 25.4 as avg_runoff,
                sum(case when avg_loss > 0 then 1 else 0 end)
                    as avg_loss_events,
                sum(case when avg_delivery > 0 then 1 else 0 end)
                    as avg_delivery_events,
                sum(case when qc_precip > 0 then 1 else 0 end)
                    as qc_precip_events,
                sum(case when avg_runoff > 0 then 1 else 0 end)
                    as avg_runoff_events
                from results_by_huc12 where huc_12 = %s and scenario = 0
                GROUP by yr ORDER by yr ASC
            """,
                conn,
                params=(huc12,),
                index_col=None,
            )
    if fmt == "xlsx":
        bio = BytesIO()
        # pylint: disable=abstract-class-instantiated
        writer = pd.ExcelWriter(bio, engine="xlsxwriter")
        df.to_excel(writer, f"{huc12} Data", index=False)
        writer.close()
        return bio.getvalue()

    res = {
        "results": [],
        "huc12": huc12,
        "generation_time": utcnow.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    for _, row in df.iterrows():
        dt = row[0]
        if isinstance(row[0], int):
            dt = datetime.date(int(row[0]), 1, 1)
        res["results"].append(
            dict(
                date=dt.strftime("%Y-%m-%d"),
                avg_loss=row["avg_loss"],
                avg_loss_events=row["avg_loss_events"],
                avg_delivery=row["avg_delivery"],
                avg_delivery_events=row["avg_delivery_events"],
                qc_precip=row["qc_precip"],
                qc_precip_events=row["qc_precip_events"],
                avg_runoff=row["avg_runoff"],
                avg_runoff_events=row["avg_runoff_events"],
            )
        )
    return json.dumps(res)


def application(environ, start_response):
    """Do Fun things"""
    form = parse_formvars(environ)
    cb = form.get("callback", None)
    huc12 = form.get("huc12", "000000000000")[:12]
    mode = form.get("mode", "daily")
    fmt = form.get("format", "json")

    if fmt == "json":
        headers = [("Content-Type", "application/vnd.geo+json")]
    elif fmt == "xlsx":
        headers = [
            ("Content-Type", EXL),
            ("Content-disposition", f"attachment; Filename=dep{huc12}.xlsx"),
        ]
    start_response("200 OK", headers)

    mckey = f"/geojson/huc12_events/{huc12}/{mode}/{fmt}"
    mc = Client(["iem-memcached", 11211])
    res = mc.get(mckey)
    if res is None:
        res = do(huc12, mode, fmt)
        if fmt == "xlsx":
            mc.close()
            return [res]
        mc.set(mckey, res, 15)
    else:
        res = res.decode("utf-8")
    mc.close()
    if cb is not None:
        res = f"{cb}({res})"
    return [res.encode("ascii")]
