"""Mapping Interface."""
import datetime
import sys
from io import BytesIO

import geopandas as gpd
import matplotlib.colors as mpcolors
import pandas as pd
from matplotlib.patches import Polygon, Rectangle
from paste.request import parse_formvars
from pyiem.dep import RAMPS
from pyiem.plot.colormaps import dep_erosion, james
from pyiem.plot.geoplot import Z_OVERLAY2, MapPlot
from pyiem.plot.use_agg import plt
from pyiem.plot.util import pretty_bins
from pyiem.reference import EPSG
from pyiem.util import get_dbconn, get_sqlalchemy_conn
from pymemcache.client import Client
from sqlalchemy import text

V2NAME = {
    "avg_loss": "Detachment",
    "qc_precip": "Precipitation",
    "avg_delivery": "Hillslope Soil Loss",
    "avg_runoff": "Runoff",
    "dt": "Dominant Tillage Code",
}
V2MULTI = {
    "avg_loss": 4.463,
    "qc_precip": 1.0 / 25.4,
    "avg_delivery": 4.463,
    "avg_runoff": 1.0 / 25.4,
    "dt": 1,
}
V2UNITS = {
    "avg_loss": "tons/acre",
    "qc_precip": "inches",
    "avg_delivery": "tons/acre",
    "avg_runoff": "inches",
    "dt": "categorical",
}


def make_overviewmap(form):
    """Draw a pretty map of just the HUC."""
    huc = form.get("huc")
    plt.close()
    projection = EPSG[5070]
    params = {}
    if huc is None:
        huclimiter = ""
    elif len(huc) >= 8:
        huclimiter = " and substr(huc_12, 1, 8) = :huc8 "
        params["huc8"] = huc[:8]
    with get_sqlalchemy_conn("idep") as conn:
        df = gpd.read_postgis(
            text(
                f"""
            SELECT simple_geom as geom, huc_12,
            ST_x(ST_Transform(ST_Centroid(geom), 4326)) as centroid_x,
            ST_y(ST_Transform(ST_Centroid(geom), 4326)) as centroid_y, name
            from huc12 i WHERE i.scenario = 0 {huclimiter}
        """
            ),
            conn,
            geom_col="geom",
            params=params,
            index_col="huc_12",
        )
    minx, miny, maxx, maxy = df["geom"].total_bounds
    buf = float(form.get("zoom", 10.0)) * 1000.0  # 10km
    hucname = "" if huc not in df.index else df.at[huc, "name"]
    subtitle = "The HUC8 is in tan"
    if len(huc) == 12:
        subtitle = "HUC12 highlighted in red, the HUC8 it resides in is in tan"
    m = MapPlot(
        axisbg="#EEEEEE",
        logo="dep",
        sector="custom",
        south=miny - buf,
        north=maxy + buf,
        west=minx - buf,
        east=maxx + buf,
        projection=projection,
        continentalcolor="white",
        title=f"DEP HUC {huc}:: {hucname}",
        subtitle=subtitle,
        titlefontsize=20,
        subtitlefontsize=18,
        caption="Daily Erosion Project",
    )
    for _huc12, row in df.iterrows():
        p = Polygon(
            row["geom"].exterior.coords,
            fc="red" if _huc12 == huc else "tan",
            ec="k",
            zorder=Z_OVERLAY2,
            lw=0.1,
        )
        m.ax.add_patch(p)
        # If this is our HUC, add some text to prevent cities overlay overlap
        if _huc12 == huc:
            m.plot_values(
                [row["centroid_x"]],
                [row["centroid_y"]],
                ["    .    "],
                color="None",
                outlinecolor="None",
            )
    if huc is not None:
        m.drawcounties()
        m.drawcities()
    ram = BytesIO()
    plt.savefig(ram, format="png", dpi=100)
    plt.close()
    ram.seek(0)
    return ram.read(), True


def label_scenario(ax, scenario, pgconn):
    """Overlay a simple label of this scenario."""
    if scenario == 0:
        return
    cursor = pgconn.cursor()
    cursor.execute("select label from scenarios where id = %s", (scenario,))
    if cursor.rowcount == 0:
        return
    label = cursor.fetchone()[0]
    ax.text(
        0.99,
        0.99,
        f"Scenario {scenario}: {label}",
        transform=ax.transAxes,
        ha="right",
        va="top",
        bbox=dict(color="white"),
        zorder=1000,
    )


def make_map(huc, ts, ts2, scenario, v, form):
    """Make the map"""
    projection = EPSG[5070]
    plt.close()
    # suggested for runoff and precip
    if v in ["qc_precip", "avg_runoff"]:
        # c = ['#ffffa6', '#9cf26d', '#76cc94', '#6399ba', '#5558a1']
        cmap = james()
    # suggested for detachment
    elif v in ["avg_loss", "dt"]:
        # c =['#cbe3bb', '#c4ff4d', '#ffff4d', '#ffc44d', '#ff4d4d', '#c34dee']
        cmap = dep_erosion()
    # suggested for delivery
    elif v in ["avg_delivery"]:
        # c =['#ffffd2', '#ffff4d', '#ffe0a5', '#eeb74d', '#ba7c57', '#96504d']
        cmap = dep_erosion()

    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()

    title = f"for {ts:%-d %B %Y}"
    if ts != ts2:
        title = f"for period between {ts:%-d %b %Y} and {ts2:%-d %b %Y}"
        if "averaged" in form:
            title = (
                f"averaged between {ts:%-d %b} and {ts2:%-d %b} (2008-2017)"
            )

    # Check that we have data for this date!
    cursor.execute(
        "SELECT value from properties where key = 'last_date_0'",
    )
    lastts = datetime.datetime.strptime(cursor.fetchone()[0], "%Y-%m-%d")
    floor = datetime.date(2007, 1, 1)
    if ts > lastts.date() or ts2 > lastts.date() or ts < floor:
        plt.text(
            0.5,
            0.5,
            "Data Not Available\nPlease Check Back Later!",
            fontsize=20,
            ha="center",
        )
        ram = BytesIO()
        plt.savefig(ram, format="png", dpi=100)
        plt.close()
        ram.seek(0)
        return ram.read(), False
    params = {
        "scenario": scenario,
        "sday1": f"{ts:%m%d}",
        "sday2": f"{ts2:%m%d}",
        "ts": ts,
        "ts2": ts2,
        "dbcol": V2MULTI[v],
    }
    if huc is None:
        huclimiter = ""
    elif len(huc) == 8:
        huclimiter = " and substr(i.huc_12, 1, 8) = :huc8 "
        params["huc8"] = huc
    elif len(huc) == 12:
        huclimiter = " and i.huc_12 = :huc12 "
        params["huc12"] = huc
    if "iowa" in form:
        huclimiter += " and i.states ~* 'IA' "
    if "mn" in form:
        huclimiter += " and i.states ~* 'MN' "
    if v == "dt":
        with get_sqlalchemy_conn("idep") as conn:
            df = gpd.read_postgis(
                text(
                    f"""
            SELECT simple_geom as geom,
            dominant_tillage as data
            from huc12 i WHERE scenario = :scenario {huclimiter}
            """
                ),
                conn,
                params=params,
                geom_col="geom",
            )
    elif "averaged" in form:
        # 11 years of data is standard
        # 10 years is for the switchgrass one-off
        with get_sqlalchemy_conn("idep") as conn:
            df = gpd.read_postgis(
                text(
                    f"""
            WITH data as (
            SELECT huc_12, sum({v}) / 10. as d from results_by_huc12
            WHERE scenario = :scenario and to_char(valid, 'mmdd') between
            :sday1 and :sday2
            and valid between '2008-01-01' and '2018-01-01'
            GROUP by huc_12)

            SELECT simple_geom as geom,
            coalesce(d.d, 0) * :dbcol as data
            from huc12 i LEFT JOIN data d
            ON (i.huc_12 = d.huc_12) WHERE i.scenario = :scenario {huclimiter}
            """
                ),
                conn,
                params=params,
                geom_col="geom",
            )

    else:
        with get_sqlalchemy_conn("idep") as conn:
            df = gpd.read_postgis(
                text(
                    f"""
            WITH data as (
            SELECT huc_12, sum({v})  as d from results_by_huc12
            WHERE scenario = :scenario and valid between :ts and :ts2
            GROUP by huc_12)

            SELECT simple_geom as geom,
            coalesce(d.d, 0) * :dbcol as data
            from huc12 i LEFT JOIN data d
            ON (i.huc_12 = d.huc_12) WHERE i.scenario = :scenario {huclimiter}
            """
                ),
                conn,
                params=params,
                geom_col="geom",
            )
    minx, miny, maxx, maxy = df["geom"].total_bounds
    buf = 10000.0  # 10km
    m = MapPlot(
        axisbg="#EEEEEE",
        logo="dep",
        sector="custom",
        south=miny - buf,
        north=maxy + buf,
        west=minx - buf,
        east=maxx + buf,
        projection=projection,
        title=f"DEP {V2NAME[v]} by HUC12 {title}",
        titlefontsize=16,
        caption="Daily Erosion Project",
    )
    if ts == ts2:
        # Daily
        bins = RAMPS["english"][0]
    else:
        bins = RAMPS["english"][1]
    # Check if our ramp makes sense
    p95 = df["data"].describe(percentiles=[0.95])["95%"]
    if not pd.isna(p95) and p95 > bins[-1]:
        bins = pretty_bins(0, p95)
        bins[0] = 0.01
    if v == "dt":
        bins = range(1, 8)
    norm = mpcolors.BoundaryNorm(bins, cmap.N)
    for _, row in df.iterrows():
        p = Polygon(
            row["geom"].exterior.coords,
            fc=cmap(norm([row["data"]]))[0],
            ec="k",
            zorder=5,
            lw=0.1,
        )
        m.ax.add_patch(p)

    label_scenario(m.ax, scenario, pgconn)

    lbl = [round(_, 2) for _ in bins]
    if huc is not None:
        m.drawcounties()
        m.drawcities()
    m.draw_colorbar(
        bins, cmap, norm, units=V2UNITS[v], clevlabels=lbl, spacing="uniform"
    )
    if "progressbar" in form:
        fig = plt.gcf()
        avgval = df["data"].mean()
        _ll = ts.year if "averaged" not in form else "Avg"
        fig.text(
            0.01,
            0.905,
            f"{_ll}: {avgval:4.1f} T/a",
            fontsize=14,
        )
        bar_width = 0.758
        # yes, a small one off with years having 366 days
        proportion = (ts2 - ts).days / 365.0 * bar_width
        rect1 = Rectangle(
            (0.15, 0.905),
            bar_width,
            0.02,
            color="k",
            zorder=40,
            transform=fig.transFigure,
            figure=fig,
        )
        fig.patches.append(rect1)
        rect2 = Rectangle(
            (0.151, 0.907),
            proportion,
            0.016,
            color=cmap(norm([avgval]))[0],
            zorder=50,
            transform=fig.transFigure,
            figure=fig,
        )
        fig.patches.append(rect2)
    if "cruse" in form:
        # Crude conversion of T/a to mm depth
        depth = avgval / 5.0
        m.ax.text(
            0.9,
            0.92,
            f"{depth:.2f}mm",
            zorder=1000,
            fontsize=24,
            transform=m.ax.transAxes,
            ha="center",
            va="center",
            bbox=dict(color="k", alpha=0.5, boxstyle="round,pad=0.1"),
            color="white",
        )
    ram = BytesIO()
    plt.savefig(ram, format="png", dpi=100)
    plt.close()
    ram.seek(0)
    return ram.read(), True


def main(environ):
    """Do something fun"""
    form = parse_formvars(environ)
    year = form.get("year", 2015)
    month = form.get("month", 5)
    day = form.get("day", 5)
    year2 = form.get("year2", year)
    month2 = form.get("month2", month)
    day2 = form.get("day2", day)
    scenario = int(form.get("scenario", 0))
    v = form.get("v", "avg_loss")
    huc = form.get("huc")

    ts = datetime.date(int(year), int(month), int(day))
    ts2 = datetime.date(int(year2), int(month2), int(day2))
    mckey = f"/auto/map.py/{huc}/{ts:%Y%m%d}/{ts2:%Y%m%d}/{scenario}/{v}"
    if form.get("overview"):
        mckey = f"/auto/map.py/{huc}/{form.get('zoom')}"
    mc = Client("iem-memcached:11211")
    res = mc.get(mckey)
    if res is None:
        # Lazy import to help speed things up
        if form.get("overview"):
            res, do_cache = make_overviewmap(form)
        else:
            res, do_cache = make_map(huc, ts, ts2, scenario, v, form)
        sys.stderr.write(f"Setting cache: {mckey}\n")
        if do_cache:
            mc.set(mckey, res, 3600)
    mc.close()
    return res


def application(environ, start_response):
    """Our mod-wsgi handler"""
    output = main(environ)
    response_headers = [("Content-type", "image/png")]
    start_response("200 OK", response_headers)

    return [output]
