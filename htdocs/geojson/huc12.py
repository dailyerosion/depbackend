"""GeoJSON service for HUC12 data"""
import datetime

# needed for Decimal formatting to work
import simplejson as json
from paste.request import parse_formvars
from pyiem.dep import RAMPS
from pyiem.util import get_dbconn
from pymemcache.client import Client


def do(ts, ts2, domain):
    """Do work"""
    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()
    utcnow = datetime.datetime.utcnow()
    dextra = "valid = %s"
    args = (ts,)
    if ts2 is not None:
        dextra = "valid >= %s and valid <= %s"
        args = (ts, ts2)
    domainextra = ""
    if domain is not None:
        domainextra = f" and states ~* '{domain[:2].upper()}'"
    cursor.execute(
        f"""
        WITH data as (
            SELECT ST_asGeoJson(ST_Transform(simple_geom, 4326), 4) as g,
            huc_12
            from huc12 WHERE scenario = 0 {domainextra}),
        obs as (
            SELECT huc_12,
            sum(coalesce(avg_loss, 0)) * 4.463 as avg_loss,
            sum(coalesce(avg_delivery, 0)) * 4.463 as avg_delivery,
            sum(coalesce(qc_precip, 0)) / 25.4 as qc_precip,
            sum(coalesce(avg_runoff, 0)) / 25.4 as avg_runoff
            from results_by_huc12 WHERE {dextra}
            and scenario = 0 GROUP by huc_12)

        SELECT d.g, d.huc_12,
        coalesce(round(o.avg_loss::numeric, 2), 0),
        coalesce(round(o.qc_precip::numeric, 2), 0),
        coalesce(round(o.avg_delivery::numeric, 2), 0),
        coalesce(round(o.avg_runoff::numeric, 2), 0)
        from data d LEFT JOIN obs o ON (d.huc_12 = o.huc_12)
    """,
        args,
    )
    res = {
        "type": "FeatureCollection",
        "date": ts.strftime("%Y-%m-%d"),
        "date2": None if ts2 is None else ts2.strftime("%Y-%m-%d"),
        "features": [],
        "generation_time": utcnow.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": cursor.rowcount,
    }
    avg_loss = []
    qc_precip = []
    avg_delivery = []
    avg_runoff = []
    for row in cursor:
        avg_loss.append(row[2])
        qc_precip.append(row[3])
        avg_delivery.append(row[4])
        avg_runoff.append(row[5])
        res["features"].append(
            dict(
                type="Feature",
                id=row[1],
                properties=dict(
                    avg_loss=row[2],
                    qc_precip=row[3],
                    avg_delivery=row[4],
                    avg_runoff=row[5],
                ),
                geometry=json.loads(row[0]),
            )
        )
    myramp = RAMPS["english"][0]
    if ts2 is not None:
        days = (ts2 - ts).days
        myramp = RAMPS["english"][1]
        if days > 31:
            myramp = RAMPS["english"][2]

    res["jenks"] = dict(
        avg_loss=myramp,
        qc_precip=myramp,
        avg_delivery=myramp,
        avg_runoff=myramp,
    )
    res["max_values"] = dict(
        avg_loss=max(avg_loss),
        qc_precip=max(qc_precip),
        avg_delivery=max(avg_delivery),
        avg_runoff=max(avg_runoff),
    )
    return json.dumps(res)


def application(environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/vnd.geo+json")]
    start_response("200 OK", headers)
    form = parse_formvars(environ)
    cb = form.get("callback", None)
    domain = form.get("domain", None)
    ts = datetime.datetime.strptime(form.get("date", "2015-05-05"), "%Y-%m-%d")
    ts2 = None
    if form.get("date2", None) is not None:
        ts2 = datetime.datetime.strptime(form.get("date2"), "%Y-%m-%d")

    tkey = "" if ts2 is None else ts2.strftime("%Y%m%d")
    dkey = "" if domain is None else domain
    mckey = f"/geojson/huc12/{ts:%Y%m%d}/{tkey}/{dkey}"
    mc = Client("iem-memcached:11211")
    res = mc.get(mckey)
    if res is None:
        res = do(ts, ts2, domain)
        try:
            # Unknown BrokenPipeError
            mc.set(mckey, res, 3600)
        except Exception as exp:
            print(exp)
    else:
        res = res.decode("utf-8")
    mc.close()
    if cb is not None:
        res = f"{cb}({res})"
    return [res.encode("ascii")]
