""".. title:: DEP Map Generation Service.

This service emits PNG map result images for a given set of parameters.

Changelog
---------

- 2026-06-17: Droped `iowa` and `mn` parameters, use state=IA or state=MN.
- 2026-06-17: Dropped `averaged` parameter as ill-formed and unused.
- 2026-06-17: Dropped `cruse` parameter as ill-formed and unused.
- 2026-06-17: Initial documentation release.

Example Requests
----------------

Provide annual averaged runoff for Kansas between 2010 and 2015

https://mesonet-dep.agron.iastate.edu/auto/mapper.py?sdate=2010-01-01&\
edate=2015-12-31&v=avg_runoff&annual=true&state=KS

"""

from datetime import date
from io import BytesIO
from typing import Annotated

import geopandas as gpd
import matplotlib.colors as mpcolors
import pandas as pd
from dailyerosion.reference import KG_M2_TO_TON_ACRE, RAMPS
from matplotlib.patches import Polygon, Rectangle
from pydantic import Field, field_validator, model_validator
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.exceptions import NoDataFound
from pyiem.plot.colormaps import dep_erosion, james
from pyiem.plot.geoplot import Z_OVERLAY2, MapPlot
from pyiem.plot.util import pretty_bins
from pyiem.reference import EPSG, state_bounds
from pyiem.webutil import CGIModel, iemapp
from pyproj import Transformer
from pyproj.crs.crs import CRS
from sqlalchemy.engine import Connection

V2NAME = {
    "avg_loss": "Detachment",
    "qc_precip": "Precipitation",
    "avg_delivery": "Hillslope Soil Delivery",
    "avg_runoff": "Runoff",
    "dt": "Dominant Tillage Code",
    "slp": "Average Slope Ratio",
}
V2MULTI = {
    "avg_loss": KG_M2_TO_TON_ACRE,
    "qc_precip": 1.0 / 25.4,
    "avg_delivery": KG_M2_TO_TON_ACRE,
    "avg_runoff": 1.0 / 25.4,
    "dt": 1,
    "slp": 1,
}
V2UNITS = {
    "avg_loss": "tons/acre",
    "qc_precip": "inches",
    "avg_delivery": "tons/acre",
    "avg_runoff": "inches",
    "dt": "categorical",
    "slp": "ratio",
}


class Schema(CGIModel):
    """See how we are called."""

    dpi: Annotated[int, Field(description="Dots per inch", ge=50, le=300)] = (
        100
    )
    sdate: Annotated[
        date | None,
        Field(description="Start date to plot", ge=date(2007, 1, 1)),
    ] = None
    edate: Annotated[
        date | None, Field(description="End date to plot", ge=date(2007, 1, 1))
    ] = None
    year: Annotated[int, Field(description="Year of start date.", ge=2007)] = (
        2024
    )
    month: Annotated[
        int, Field(description="Month of start date.", ge=1, le=12)
    ] = 1
    day: Annotated[
        int, Field(description="Day of start date.", ge=1, le=31)
    ] = 1
    year2: Annotated[
        int | None, Field(description="Year of end date.", ge=2007)
    ] = None
    month2: Annotated[
        int | None, Field(description="Month of end date.", ge=1, le=12)
    ] = None
    day2: Annotated[
        int | None, Field(description="Day of end date.", ge=1, le=31)
    ] = None
    scenario: Annotated[int, Field(description="Scenario ID", ge=0)] = 0
    v: Annotated[str, Field(description="Variable to plot")] = "avg_loss"
    huc: Annotated[
        str | None,
        Field(
            pattern=r"^\d{8}(\d{4})?$",
            description="HUC8 or HUC12 to plot, required when overview is set",
        ),
    ] = None
    state: Annotated[
        str | None,
        Field(
            pattern="^[A-Z]{2}$",
            description=(
                "If provided, zoom the map to the provided state abbreviation."
            ),
        ),
    ] = None
    extent: Annotated[
        list[float],
        Field(
            default_factory=list,
            description=(
                "Custom map extent as [west, south, east, north] in lat/lon. "
                "``state`` parameter used before this."
            ),
        ),
    ]
    zoom: Annotated[float, Field(description="Zoom level")] = 10.0
    overview: Annotated[bool, Field(description="Generate overview map")] = (
        False
    )
    progressbar: Annotated[bool, Field(description="Show progress bar")] = (
        False
    )
    annual: Annotated[
        bool,
        Field(
            description=(
                "If variable allows, produce annual averages by dividing by "
                "the number of years between sdate and edate"
            ),
        ),
    ] = False

    @field_validator("extent", mode="before")
    def ensure_extent_is_valid(cls, v):
        """Ensure that if extent is provided, it is valid."""
        if not v:
            return v
        tokens = v.split(",")
        if len(tokens) != 4:
            raise ValueError("Extent must be a list of 4 floats")
        return [float(val) for val in tokens]

    @model_validator(mode="after")
    @classmethod
    def ensure_huc_with_overview(cls, model):
        """If overview is requested, ensure we have a HUC."""
        if model.overview and model.huc is None:
            raise ValueError("Overview maps require a HUC")
        return model

    @model_validator(mode="after")
    @classmethod
    def merge_provided_dates(cls, model):
        """Ensure sdate and edate eventually get set."""
        if model.sdate is None:
            model.sdate = date(model.year, model.month, model.day)
        if model.edate is None:
            if (
                model.year2 is not None
                and model.month2 is not None
                and model.day2 is not None
            ):
                model.edate = date(model.year2, model.month2, model.day2)
            else:
                model.edate = model.sdate
        return model


def make_overviewmap(query: Schema):
    """Draw a pretty map of just the HUC."""
    projection = EPSG[5070]
    params = {}
    huclimiter = ""
    if len(query.huc) >= 8:
        huclimiter = " and substr(huc_12, 1, 8) = :huc8 "
        params["huc8"] = query.huc[:8]
    with get_sqlalchemy_conn("idep") as conn:
        df = gpd.read_postgis(
            sql_helper(
                """
            SELECT simple_geom as geom, huc_12,
            ST_x(ST_Transform(ST_Centroid(geom), 4326)) as centroid_x,
            ST_y(ST_Transform(ST_Centroid(geom), 4326)) as centroid_y, name
            from huc12 i WHERE i.scenario = 0 {huclimiter}
        """,
                huclimiter=huclimiter,
            ),
            conn,
            geom_col="geom",
            params=params,
            index_col="huc_12",
        )  # type: ignore
    if df.empty:
        raise NoDataFound("No Data Found for this scenario and date")
    minx, miny, maxx, maxy = df["geom"].total_bounds
    buf = query.zoom * 1000.0  # 10km
    hucname = "" if query.huc not in df.index else df.at[query.huc, "name"]
    subtitle = "The HUC8 is in tan"
    if len(query.huc) == 12:
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
        title=f"DEP HUC {query.huc}:: {hucname}",
        subtitle=subtitle,
        titlefontsize=20,
        subtitlefontsize=18,
        caption="Daily Erosion Project",
    )
    for _huc12, row in df.iterrows():
        p = Polygon(
            row["geom"].exterior.coords,
            fc="red" if _huc12 == query.huc else "tan",
            ec="k",
            zorder=Z_OVERLAY2,
            lw=0.1,
        )
        m.ax.add_patch(p)
        # If this is our HUC, add some text to prevent cities overlay overlap
        if _huc12 == query.huc:
            m.plot_values(
                [row["centroid_x"]],
                [row["centroid_y"]],
                ["    .    "],
                color="None",
                outlinecolor="None",
            )
    if query.huc is not None:
        m.drawcounties()
        m.drawcities()
    ram = BytesIO()
    m.fig.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    return ram


def label_scenario(ax, scenario, conn):
    """Overlay a simple label of this scenario."""
    if scenario == 0:
        return
    res = conn.execute(
        sql_helper("select label from scenarios where id = :id"),
        {"id": scenario},
    )
    if res.rowcount == 0:
        return
    label = res.fetchone()[0]
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


def build_map_object(query: Schema, df: pd.DataFrame):
    """Figure out the map instance to generate."""
    title = f"for {query.sdate:%-d %B %Y}"
    if query.sdate != query.edate:
        title = f"for period between {query.sdate:%-d %b %Y} and {query.edate:%-d %b %Y}"

    projection: CRS = EPSG[5070]
    buf = 10000.0  # 10km
    if not df.empty:
        minx, miny, maxx, maxy = df["geom"].total_bounds
    else:
        # meh
        minx, miny, maxx, maxy = -678439, 1551480, 677645, 2888203.0

    # If state is provided, zoom to that state instead
    if query.state is not None:
        minx, miny, maxx, maxy = state_bounds[query.state]
        # Reproject to our map projection
        transformer = Transformer.from_crs(
            EPSG[4326], projection, always_xy=True
        )
        minx, miny = transformer.transform(minx, miny)
        maxx, maxy = transformer.transform(maxx, maxy)

    elif query.extent:
        minx, miny, maxx, maxy = query.extent
        # Reproject to our map projection
        transformer = Transformer.from_crs(
            EPSG[4326], projection, always_xy=True
        )
        minx, miny = transformer.transform(minx, miny)
        maxx, maxy = transformer.transform(maxx, maxy)

    aec = " per year" if query.annual else ""

    return MapPlot(
        axisbg="#EEEEEE",
        logo="dep",
        sector="custom",
        south=miny - buf,
        north=maxy + buf,
        west=minx - buf,
        east=maxx + buf,
        projection=projection,
        title=f"DEP {V2NAME[query.v]} by HUC12 {title}{aec}",
        titlefontsize=16,
        caption="Daily Erosion Project",
    )


def get_map_data(query: Schema, conn: Connection) -> gpd.GeoDataFrame:
    """Figure out the data for this query."""
    # Compute what the huc12 scenario is for this scenario
    res = conn.execute(
        sql_helper("select huc12_scenario from scenarios where id = :id"),
        {"id": query.scenario},
    )
    huc12_scenario = res.fetchone()[0]

    params = {
        "scenario": query.scenario,
        "huc12_scenario": huc12_scenario,
        "sday1": f"{query.sdate:%m%d}",
        "sday2": f"{query.edate:%m%d}",
        "ts": query.sdate,
        "ts2": query.edate,
        "dbcol": V2MULTI[query.v],
        "state": query.state,
    }
    huclimiter = ""
    if query.huc is not None:
        if len(query.huc) == 8:
            huclimiter = " and substr(i.huc_12, 1, 8) = :huc8 "
            params["huc8"] = query.huc
        elif len(query.huc) == 12:
            huclimiter = " and i.huc_12 = :huc12 "
            params["huc12"] = query.huc
    if query.state:
        huclimiter += " and i.states ~* :state "
    if query.v in ["dt", "slp"]:
        colname = (
            "dominant_tillage" if query.v == "dt" else "average_slope_ratio"
        )
        df = gpd.read_postgis(
            sql_helper(
                """
        SELECT simple_geom as geom,
        {colname} as data
        from huc12 i WHERE scenario = :huc12_scenario {huclimiter}
        """,
                huclimiter=huclimiter,
                colname=colname,
            ),
            conn,
            params=params,
            geom_col="geom",
        )  # type: ignore
    else:
        df = gpd.read_postgis(
            sql_helper(
                """
        WITH data as (
        SELECT huc_12, sum({v})  as d from results_by_huc12
        WHERE scenario = :scenario and valid between :ts and :ts2
        GROUP by huc_12)

        SELECT simple_geom as geom,
        coalesce(d.d, 0) * :dbcol as data
        from huc12 i LEFT JOIN data d
        ON (i.huc_12 = d.huc_12) WHERE i.scenario = :huc12_scenario
        {huclimiter}
        """,
                huclimiter=huclimiter,
                v=query.v,
            ),
            conn,
            params=params,
            geom_col="geom",
        )  # type: ignore
        if query.annual:
            years = query.edate.year - query.sdate.year + 1
            df["data"] = df["data"] / years
    return df


def make_map(conn, query: Schema):
    """Make the map"""
    # suggested for runoff and precip
    if query.v in ["qc_precip", "avg_runoff"]:
        cmap = james()
    # suggested for detachment
    else:
        cmap = dep_erosion()

    df = get_map_data(query, conn)
    mp = build_map_object(query, df)

    if query.sdate == query.edate:
        # Daily
        bins = RAMPS["english"][0]
    else:
        bins = RAMPS["english"][1]
    # Check if our ramp makes sense
    if not df.empty:
        p95 = df["data"].describe(percentiles=[0.95])["95%"]
        if not pd.isna(p95) and p95 > bins[-1]:
            bins = pretty_bins(0, p95)
            bins[0] = 0.01
        if query.v == "dt":
            bins = range(1, 8)
        if query.v == "slp":
            bins = [0, 0.01, 0.03, 0.05, 0.07, 0.1, 0.5]
        norm = mpcolors.BoundaryNorm(bins, cmap.N)
        for _, row in df.to_crs(mp.panels[0].crs).iterrows():
            p = Polygon(
                row["geom"].exterior.coords,
                fc=cmap(norm([row["data"]]))[0],
                ec="k",
                zorder=5,
                lw=0.1,
            )
            mp.ax.add_patch(p)
        lbl = [round(_, 2) for _ in bins]
        mp.draw_colorbar(
            bins,
            cmap,
            norm,
            units=V2UNITS[query.v] + "/year" if query.annual else "",
            clevlabels=lbl,
            spacing="uniform",
        )

    label_scenario(mp.ax, query.scenario, conn)

    if query.huc is not None:
        mp.drawcounties()
        mp.drawcities()
    avgval = None
    if query.progressbar:
        avgval = df["data"].mean()
        _ll = query.sdate.year
        mp.fig.text(
            0.06,
            0.905,
            f"{_ll}: {avgval:4.1f} T/a",
            fontsize=14,
        )
        bar_width = 0.698
        # yes, a small one off with years having 366 days
        proportion = (query.edate - query.sdate).days / 365.0 * bar_width
        rect1 = Rectangle(
            (0.20, 0.905),
            bar_width,
            0.02,
            color="k",
            zorder=40,
            transform=mp.fig.transFigure,
            figure=mp.fig,
        )
        mp.fig.patches.append(rect1)
        rect2 = Rectangle(
            (0.201, 0.907),
            proportion,
            0.016,
            color=cmap(norm(avgval)),
            zorder=50,
            transform=mp.fig.transFigure,
            figure=mp.fig,
        )
        mp.fig.patches.append(rect2)
    ram = BytesIO()
    mp.fig.savefig(ram, format="png", dpi=query.dpi)
    ram.seek(0)
    return ram


@iemapp(
    content_type="image/png",
    help=__doc__,
    schema=Schema,
    parse_times=False,
)
def application(environ: dict, start_response: callable):
    """Our mod-wsgi handler"""
    # Capture the request
    query: Schema = environ["_cgimodel_schema"]
    if query.overview:
        res = make_overviewmap(query).read()
    else:
        with get_sqlalchemy_conn("idep") as conn:
            res = make_map(conn, query).read()

    # Ensure that all work is done before we start to respond.
    start_response("200 OK", [("Content-type", "image/png")])
    return res
