"""
Called from DEP map application
"""
import datetime
import os
import tempfile
import zipfile

from geopandas import GeoDataFrame
from paste.request import parse_formvars
from pyiem.util import get_sqlalchemy_conn
from sqlalchemy import text

PRJFILE = "/opt/iem/data/gis/meta/5070.prj"


def workflow(start_response, dt, dt2, states):
    """Generate for a given date"""
    dextra = "valid = :dt"
    params = {"dt": dt}
    if dt2 is not None:
        dextra = "valid >= :dt and valid <= :dt2"
        params["dt2"] = dt2
    statelimit = ""
    if states is not None:
        tokens = states.split(",")
        if tokens:
            _s = [f" states ~* '{a}' " for a in tokens]
            statelimit = " and (" + " or ".join(_s) + " ) "
    with get_sqlalchemy_conn("idep") as conn:
        df = GeoDataFrame.from_postgis(
            text(
                f"""
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
        """
            ),
            conn,
            params=params,
            geom_col="geo",
        )

    with tempfile.TemporaryDirectory() as tempdir:
        os.chdir(tempdir)
        fn = f"idepv2_{dt:%Y%m%d}"
        if dt2:
            fn += dt2.strftime("_%Y%m%d")
        df.columns = [
            s.upper() if s != "geo" else s for s in df.columns.values
        ]
        df.to_file(f"{fn}.shp")
        df.drop(columns="geo").to_csv(f"{fn}.csv", index=False)
        with zipfile.ZipFile(fn + ".zip", "w", zipfile.ZIP_DEFLATED) as zfp:
            with open(PRJFILE, encoding="ascii") as fh:
                zfp.writestr(f"{fn}.prj", fh.read())
            for suffix in ["shp", "shx", "dbf", "csv"]:
                zfp.write(f"{fn}.{suffix}")

        with open(f"{fn}.zip", "rb") as fh:
            res = fh.read()
    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", f"attachment; filename={fn}.zip"),
    ]
    start_response("200 OK", headers)

    return res


def application(environ, start_response):
    """Generate something nice for the users"""
    form = parse_formvars(environ)
    dt = datetime.datetime.strptime(form.get("dt", "2019-12-11"), "%Y-%m-%d")
    dt2 = form.get("dt2")
    states = form.get("states")
    if dt2 is not None:
        dt2 = datetime.datetime.strptime(form.get("dt2"), "%Y-%m-%d")
    return [workflow(start_response, dt, dt2, states)]
