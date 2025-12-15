""".. title:: Download IDEP Shapefile

Emits a zip file containing a shapefile of the IDEP HUC12

"""

import datetime
import tempfile
import zipfile

from geopandas import GeoDataFrame
from pydantic import Field
from pydep.reference import KG_M2_TO_TON_ACRE
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.webutil import CGIModel, ListOrCSVType, iemapp

PRJFILE = "/opt/iem/data/gis/meta/5070.prj"


class Schema(CGIModel):
    """See how we are called."""

    dt: datetime.date = Field(..., description="Date to query")
    dt2: datetime.date = Field(None, description="Optional end date")
    states: ListOrCSVType = Field(
        None, description="Optional comma delimited states"
    )
    conv: str = Field("metric", description="Output units, metric or english")


def workflow(start_response, dt, dt2, states, conv):
    """Generate for a given date"""
    dextra = "valid = :dt"
    params = {"dt": dt}
    if dt2 is not None:
        dextra = "valid >= :dt and valid <= :dt2"
        params["dt2"] = dt2
    statelimit = ""
    if states:
        _s = [f" states ~* '{a[:2]}' " for a in states]
        statelimit = " and (" + " or ".join(_s) + " ) "
    with get_sqlalchemy_conn("idep") as conn:
        df = GeoDataFrame.from_postgis(
            sql_helper(
                """
            with data as (
                SELECT simple_geom, huc_12, name, dominant_tillage,
                average_slope_ratio, s.dep_version_label as version
                from huc12 h, scenarios s WHERE h.scenario = 0 and s.id = 0
                {statelimit}),
            obs as (
                SELECT huc_12,
                sum(coalesce(avg_loss, 0)) as avg_loss,
                sum(coalesce(avg_delivery, 0)) as avg_delivery,
                sum(coalesce(qc_precip, 0)) as qc_precip,
                sum(coalesce(avg_runoff, 0)) as avg_runoff
                from results_by_huc12 WHERE {dextra} and scenario = 0
                GROUP by huc_12)

            SELECT d.simple_geom as geo, d.huc_12, d.name,
            d.dominant_tillage as tillcode,
            d.average_slope_ratio as avg_slp1,
            coalesce(o.qc_precip, 0) as prec_mm,
            coalesce(o.avg_loss, 0) as los_kgm2,
            coalesce(o.avg_runoff, 0) as runof_mm,
            coalesce(o.avg_delivery, 0) as deli_kgm,
            d.version
            from data d LEFT JOIN obs o ON (d.huc_12 = o.huc_12)
        """,
                statelimit=statelimit,
                dextra=dextra,
            ),
            conn,
            params=params,
            geom_col="geo",
        )
    if conv == "english":
        df["prec_in"] = df["prec_mm"] / 25.4
        df["loss_tpa"] = df["los_kgm2"] * KG_M2_TO_TON_ACRE
        df["runof_in"] = df["runof_mm"] / 25.4
        df["deli_tpa"] = df["deli_kgm"] * KG_M2_TO_TON_ACRE
        df = df.drop(columns=["prec_mm", "los_kgm2", "runof_mm", "deli_kgm"])

    with tempfile.TemporaryDirectory() as tempdir:
        basefn = f"idepv2_{dt:%Y%m%d}"
        if dt2:
            basefn += dt2.strftime("_%Y%m%d")
        fn = f"{tempdir}/{basefn}"
        df.columns = [
            s.upper() if s != "geo" else s for s in df.columns.values
        ]
        df.to_file(f"{fn}.shp")
        df.drop(columns="geo").to_csv(f"{fn}.csv", index=False)
        zfn = f"{fn}.zip"
        with zipfile.ZipFile(zfn, "w", zipfile.ZIP_DEFLATED) as zfp:
            with open(PRJFILE, encoding="ascii") as fh:
                zfp.writestr(f"{basefn}.prj", fh.read())
            for suffix in ["shp", "shx", "dbf", "csv"]:
                zfp.write(f"{fn}.{suffix}", f"{basefn}.{suffix}")

        with open(zfn, "rb") as fh:
            res = fh.read()
    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={basefn}.zip"),
    ]
    start_response("200 OK", headers)

    return res


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Generate something nice for the users"""
    dt = environ["dt"]
    dt2 = environ["dt2"]
    states = environ["states"]
    conv = environ["conv"]
    return [workflow(start_response, dt, dt2, states, conv)]
