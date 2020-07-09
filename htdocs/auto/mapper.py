"""Mapping Interface."""
import sys
import datetime
from io import BytesIO

import numpy as np
import memcache
from paste.request import parse_formvars
from matplotlib.patches import Polygon, Rectangle
import matplotlib.colors as mpcolors
from geopandas import read_postgis
import cartopy.crs as ccrs
from pyiem.plot.use_agg import plt
from pyiem.plot.geoplot import MapPlot, Z_OVERLAY2
from pyiem.plot.colormaps import james, dep_erosion
from pyiem.util import get_dbconn
from pyiem.dep import RAMPS

V2NAME = {
    "avg_loss": "Detachment",
    "qc_precip": "Precipitation",
    "avg_delivery": "Hillslope Soil Loss",
    "avg_runoff": "Runoff",
}
V2MULTI = {
    "avg_loss": 4.463,
    "qc_precip": 1.0 / 25.4,
    "avg_delivery": 4.463,
    "avg_runoff": 1.0 / 25.4,
}
V2UNITS = {
    "avg_loss": "tons/acre",
    "qc_precip": "inches",
    "avg_delivery": "tons/acre",
    "avg_runoff": "inches",
}


def make_overviewmap(form):
    """Draw a pretty map of just the HUC."""
    huc = form.get("huc")
    plt.close()
    projection = ccrs.Mercator()
    if huc is None:
        huclimiter = ""
    elif len(huc) >= 8:
        huclimiter = " and substr(huc_12, 1, 8) = '%s' " % (huc[:8],)
    pgconn = get_dbconn("idep")
    df = read_postgis(
        """
        SELECT ST_Transform(simple_geom, %s) as geom, huc_12,
        ST_x(ST_Transform(ST_Centroid(geom), 4326)) as centroid_x,
        ST_y(ST_Transform(ST_Centroid(geom), 4326)) as centroid_y,
        hu_12_name
        from huc12 i WHERE i.scenario = 0 """
        + huclimiter
        + """
    """,
        pgconn,
        params=(3857,),
        geom_col="geom",
        index_col="huc_12",
    )
    minx, miny, maxx, maxy = df["geom"].total_bounds
    buf = float(form.get("zoom", 10.0)) * 1000.0  # 10km
    pts = ccrs.Geodetic().transform_points(
        projection,
        np.asarray([minx - buf, maxx + buf]),
        np.asarray([miny - buf, maxy + buf]),
    )
    hucname = "" if huc not in df.index else df.at[huc, "hu_12_name"]
    subtitle = "The HUC8 is in tan"
    if len(huc) == 12:
        subtitle = "HUC12 highlighted in red, the HUC8 it resides in is in tan"
    m = MapPlot(
        axisbg="#EEEEEE",
        nologo=True,
        sector="custom",
        south=pts[0, 1],
        north=pts[1, 1],
        west=pts[0, 0],
        east=pts[1, 0],
        projection=projection,
        continentalcolor="white",
        title="DEP HUC %s:: %s" % (huc, hucname),
        subtitle=subtitle,
        titlefontsize=20,
        subtitlefontsize=18,
        caption="Daily Erosion Project",
    )
    for _huc12, row in df.iterrows():
        p = Polygon(
            row["geom"].exterior,
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
        "Scenario %s: %s" % (scenario, label),
        transform=ax.transAxes,
        ha="right",
        va="top",
        bbox=dict(color="white"),
        zorder=1000,
    )


def make_map(huc, ts, ts2, scenario, v, form):
    """Make the map"""
    plt.close()
    # suggested for runoff and precip
    if v in ["qc_precip", "avg_runoff"]:
        # c = ['#ffffa6', '#9cf26d', '#76cc94', '#6399ba', '#5558a1']
        cmap = james()
    # suggested for detachment
    elif v in ["avg_loss"]:
        # c =['#cbe3bb', '#c4ff4d', '#ffff4d', '#ffc44d', '#ff4d4d', '#c34dee']
        cmap = dep_erosion()
    # suggested for delivery
    elif v in ["avg_delivery"]:
        # c =['#ffffd2', '#ffff4d', '#ffe0a5', '#eeb74d', '#ba7c57', '#96504d']
        cmap = dep_erosion()

    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()

    title = "for %s" % (ts.strftime("%-d %B %Y"),)
    if ts != ts2:
        title = "for period between %s and %s" % (
            ts.strftime("%-d %b %Y"),
            ts2.strftime("%-d %b %Y"),
        )
        if "averaged" in form:
            title = "averaged between %s and %s (2008-2017)" % (
                ts.strftime("%-d %b"),
                ts2.strftime("%-d %b"),
            )

    projection = ccrs.Mercator()

    # Check that we have data for this date!
    cursor.execute(
        """
        SELECT value from properties where key = 'last_date_0'
    """
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
    if huc is None:
        huclimiter = ""
    elif len(huc) == 8:
        huclimiter = " and substr(i.huc_12, 1, 8) = '%s' " % (huc,)
    elif len(huc) == 12:
        huclimiter = " and i.huc_12 = '%s' " % (huc,)
    if "iowa" in form:
        huclimiter += " and i.states ~* 'IA' "
    if "mn" in form:
        huclimiter += " and i.states ~* 'MN' "
    if "averaged" in form:
        # 11 years of data is standard
        # 10 years is for the switchgrass one-off
        df = read_postgis(
            """
        WITH data as (
        SELECT huc_12, sum("""
            + v
            + """) / 10. as d from results_by_huc12
        WHERE scenario = %s and to_char(valid, 'mmdd') between %s and %s
        and valid between '2008-01-01' and '2018-01-01'
        GROUP by huc_12)

        SELECT ST_Transform(simple_geom, %s) as geom,
        coalesce(d.d, 0) * %s as data
        from huc12 i LEFT JOIN data d
        ON (i.huc_12 = d.huc_12) WHERE i.scenario = %s
        """
            + huclimiter
            + """
        """,
            pgconn,
            params=(
                scenario,
                ts.strftime("%m%d"),
                ts2.strftime("%m%d"),
                3857,
                V2MULTI[v],
                0,
            ),
            geom_col="geom",
        )

    else:
        df = read_postgis(
            """
        WITH data as (
        SELECT huc_12, sum("""
            + v
            + """)  as d from results_by_huc12
        WHERE scenario = %s and valid between %s and %s
        GROUP by huc_12)

        SELECT ST_Transform(simple_geom, %s) as geom,
        coalesce(d.d, 0) * %s as data
        from huc12 i LEFT JOIN data d
        ON (i.huc_12 = d.huc_12) WHERE i.scenario = %s
        """
            + huclimiter
            + """
        """,
            pgconn,
            params=(
                scenario,
                ts.strftime("%Y-%m-%d"),
                ts2.strftime("%Y-%m-%d"),
                3857,
                V2MULTI[v],
                0,
            ),
            geom_col="geom",
        )
    minx, miny, maxx, maxy = df["geom"].total_bounds
    buf = 10000.0  # 10km
    pts = ccrs.Geodetic().transform_points(
        projection,
        np.asarray([minx - buf, maxx + buf]),
        np.asarray([miny - buf, maxy + buf]),
    )
    m = MapPlot(
        axisbg="#EEEEEE",
        nologo=True,
        sector="custom",
        south=pts[0, 1],
        north=pts[1, 1],
        west=pts[0, 0],
        east=pts[1, 0],
        projection=projection,
        title="DEP %s by HUC12 %s" % (V2NAME[v], title),
        titlefontsize=16,
        caption="Daily Erosion Project",
    )
    if ts == ts2:
        # Daily
        bins = RAMPS["english"][0]
    else:
        bins = RAMPS["english"][1]
    norm = mpcolors.BoundaryNorm(bins, cmap.N)
    for _, row in df.iterrows():
        p = Polygon(
            row["geom"].exterior,
            fc=cmap(norm([row["data"]]))[0],
            ec="k",
            zorder=2,
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
        fig.text(
            0.01,
            0.905,
            "%s: %4.1f T/a"
            % (ts.year if "averaged" not in form else "Avg", avgval),
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
            0.5,
            0.5,
            "%.2fmm" % (depth,),
            zorder=1000,
            fontsize=40,
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
    mckey = "/auto/map.py/%s/%s/%s/%s/%s" % (
        huc,
        ts.strftime("%Y%m%d"),
        ts2.strftime("%Y%m%d"),
        scenario,
        v,
    )
    if form.get("overview"):
        mckey = "/auto/map.py/%s/%s" % (huc, form.get("zoom"))
    mc = memcache.Client(["iem-memcached:11211"], debug=0)
    res = mc.get(mckey)
    hostname = environ.get("SERVER_NAME", "")
    if not res or hostname == "depbackend.local":
        # Lazy import to help speed things up
        if form.get("overview"):
            res, do_cache = make_overviewmap(form)
        else:
            res, do_cache = make_map(huc, ts, ts2, scenario, v, form)
        sys.stderr.write("Setting cache: %s\n" % (mckey,))
        if do_cache:
            mc.set(mckey, res, 3600)
    return res


def application(environ, start_response):
    """Our mod-wsgi handler"""
    output = main(environ)
    response_headers = [("Content-type", "image/png")]
    start_response("200 OK", response_headers)

    return [output]
