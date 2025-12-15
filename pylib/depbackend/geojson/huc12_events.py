"""GeoJSON service for HUC12 data"""

import json
from io import BytesIO

import pandas as pd
from pydantic import Field
from pydep.reference import KG_M2_TO_TON_ACRE
from pyiem.database import get_sqlalchemy_conn
from pyiem.util import utc
from pyiem.webutil import CGIModel, iemapp
from pymemcache.client import Client

EXL = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class Schema(CGIModel):
    """See how we are called."""

    callback: str = Field(None, description="JSONP callback function")
    huc12: str = Field(
        "000000000000",
        description="HUC12 identifier",
        min_length=12,
        max_length=12,
    )
    mode: str = Field("daily", description="daily or yearly summary")
    format: str = Field("json", description="json or xlsx")


def do(huc12: str, mode: str, fmt: str):
    """Do work"""
    utcnow = utc()
    if mode == "daily":
        with get_sqlalchemy_conn("idep") as conn:
            df = pd.read_sql(
                """
                SELECT valid,
                avg_loss * %s as avg_loss,
                avg_delivery * %s as avg_delivery,
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
                params=(KG_M2_TO_TON_ACRE, KG_M2_TO_TON_ACRE, huc12),
                index_col=None,
            )
    else:
        with get_sqlalchemy_conn("idep") as conn:
            df = pd.read_sql(
                """
                SELECT extract(year from valid)::int as yr,
                sum(avg_loss) * %s as avg_loss,
                sum(avg_delivery) * %s as avg_delivery,
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
                params=(KG_M2_TO_TON_ACRE, KG_M2_TO_TON_ACRE, huc12),
                index_col=None,
            )
            df["valid"] = pd.to_datetime(df["yr"].astype(str) + "-01-01")
    if fmt == "xlsx":
        # Drop unuseful event columns for daily output
        if mode == "daily":
            df = df.drop(
                columns=[
                    "avg_loss_events",
                    "avg_delivery_events",
                    "qc_precip_events",
                    "avg_runoff_events",
                ]
            )
        bio = BytesIO()
        # pylint: disable=abstract-class-instantiated
        writer = pd.ExcelWriter(bio, engine="xlsxwriter")
        df.to_excel(writer, sheet_name=f"{huc12} Data", index=False)
        writer.close()
        return bio.getvalue()

    res = {
        "results": [],
        "huc12": huc12,
        "generation_time": utcnow.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    for _, row in df.iterrows():
        res["results"].append(
            dict(
                date=row["valid"].strftime("%Y-%m-%d"),
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


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do Fun things"""
    cb = environ["callback"]
    huc12 = environ["huc12"]
    mode = environ["mode"]
    fmt = environ["format"]

    if fmt == "json":
        headers = [("Content-Type", "application/vnd.geo+json")]
    elif fmt == "xlsx":
        headers = [
            ("Content-Type", EXL),
            ("Content-disposition", f"attachment; Filename=dep{huc12}.xlsx"),
        ]
    start_response("200 OK", headers)

    mckey = f"/geojson/huc12_events/{huc12}/{mode}/{fmt}"
    mc = Client("iem-memcached:11211")
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
    return res
