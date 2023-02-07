"""
Called from DEP map application
"""
import datetime
import os
import shutil
import zipfile

from geopandas import GeoDataFrame
from paste.request import parse_formvars
from pyiem.util import get_dbconn


def workflow(start_response, dt, dt2, states):
    """Generate for a given date"""
    dbconn = get_dbconn("idep")
    dextra = "valid = %s"
    args = (dt,)
    if dt2 is not None:
        dextra = "valid >= %s and valid <= %s"
        args = (dt, dt2)
    statelimit = ""
    if states is not None:
        tokens = states.split(",")
        if tokens:
            _s = [" states ~* '%s' " % (a,) for a in tokens]
            statelimit = " and (" + " or ".join(_s) + " ) "
    df = GeoDataFrame.from_postgis(
        f"""
        WITH data as (
            SELECT simple_geom, huc_12, name
            from huc12 WHERE scenario = 0
            {statelimit}),
        obs as (
            SELECT huc_12,
            sum(coalesce(avg_loss, 0)) as avg_loss,
            sum(coalesce(avg_delivery, 0)) as avg_delivery,
            sum(coalesce(qc_precip, 0)) as qc_precip,
            sum(coalesce(avg_runoff, 0)) as avg_runoff
            from results_by_huc12 WHERE
            {dextra} and scenario = 0 GROUP by huc_12)

        SELECT d.simple_geom, d.huc_12, d.name,
        coalesce(o.qc_precip, 0) as prec_mm,
        coalesce(o.avg_loss, 0) as los_kgm2,
        coalesce(o.avg_runoff, 0) as runof_mm,
        coalesce(o.avg_delivery, 0) as deli_kgm
        from data d LEFT JOIN obs o ON (d.huc_12 = o.huc_12)
    """,
        dbconn,
        params=args,
        geom_col="simple_geom",
    )

    os.chdir("/tmp")
    fn = "idepv2_%s" % (dt.strftime("%Y%m%d"),)
    if dt2:
        fn += dt2.strftime("_%Y%m%d")
    df.columns = [
        s.upper() if s != "simple_geom" else s for s in df.columns.values
    ]
    df.to_file(fn + ".shp")
    shutil.copyfile("/opt/iem/data/gis/meta/5070.prj", fn + ".prj")
    zfp = zipfile.ZipFile(fn + ".zip", "w", zipfile.ZIP_DEFLATED)
    suffixes = ["shp", "shx", "dbf", "prj"]
    for suffix in suffixes:
        zfp.write("%s.%s" % (fn, suffix))
    zfp.close()

    headers = [
        ("Content-type", "application/octet-stream"),
        ("Content-Disposition", "attachment; filename=%s.zip" % (fn,)),
    ]
    start_response("200 OK", headers)
    res = open(fn + ".zip", "rb").read()

    suffixes.append("zip")
    for suffix in suffixes:
        os.remove("%s.%s" % (fn, suffix))
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
