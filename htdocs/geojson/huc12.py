"""GeoJSON service for HUC12 data"""

import datetime

# needed for Decimal formatting to work
import simplejson as json
from pydantic import Field
from pyiem.database import get_sqlalchemy_conn
from pyiem.dep import RAMPS
from pyiem.util import logger, utc
from pyiem.webutil import CGIModel, iemapp
from sqlalchemy import text

LOG = logger()


class Schema(CGIModel):
    """See how we are called."""

    callback: str = Field(None, description="JSONP callback function")
    date: datetime.date = Field(..., description="Date to query")
    date2: datetime.date = Field(
        None, description="Optional end date to query"
    )
    domain: str = Field(None, description="Optional domain to query")


def do(ts, ts2, domain):
    """Do work"""
    utcnow = utc()
    dextra = "valid = :date"
    params = {
        "date": ts,
        "date2": ts2,
    }
    if ts2 is not None:
        dextra = "valid >= :date and valid <= :date2"
    domainextra = ""
    if domain is not None:
        domainextra = " and states ~* :states "
        params["states"] = domain[:2].upper()
    with get_sqlalchemy_conn("idep") as conn:
        # Get version label
        res = conn.execute(
            text("SELECT dep_version_label from scenarios where id = 0")
        )
        dep_version_label = res.fetchone()[0]
        res = conn.execute(
            text(f"""
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
        """),
            params,
        )
        data = {
            "type": "FeatureCollection",
            "dep_version_label": dep_version_label,
            "date": ts.strftime("%Y-%m-%d"),
            "date2": None if ts2 is None else ts2.strftime("%Y-%m-%d"),
            "features": [],
            "generation_time": utcnow.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "count": res.rowcount,
        }
        avg_loss = []
        qc_precip = []
        avg_delivery = []
        avg_runoff = []
        for row in res:
            avg_loss.append(row[2])
            qc_precip.append(row[3])
            avg_delivery.append(row[4])
            avg_runoff.append(row[5])
            data["features"].append(
                dict(
                    type="Feature",
                    id=row[1],
                    properties=dict(
                        huc_12=row[1],
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

    data["jenks"] = dict(
        avg_loss=myramp,
        qc_precip=myramp,
        avg_delivery=myramp,
        avg_runoff=myramp,
    )
    data["max_values"] = dict(
        avg_loss=max(avg_loss),
        qc_precip=max(qc_precip),
        avg_delivery=max(avg_delivery),
        avg_runoff=max(avg_runoff),
    )
    return json.dumps(data)


def get_mckey(environ):
    """Figure out the memcache key"""
    ts = environ["date"]
    ts2 = environ["date2"]
    domain = environ["domain"]
    tkey = "" if ts2 is None else ts2.strftime("%Y%m%d")
    dkey = "" if domain is None else domain
    return f"/geojson/huc12/{ts:%Y%m%d}/{tkey}/{dkey}"


@iemapp(
    content_type="application/vnd.geo+json",
    memcachekey=get_mckey,
    help=__doc__,
    schema=Schema,
)
def application(environ, start_response):
    """Do Fun things"""
    headers = [("Content-Type", "application/vnd.geo+json")]
    start_response("200 OK", headers)
    domain = environ["domain"]
    ts = environ["date"]
    ts2 = environ["date2"]

    return do(ts, ts2, domain)
