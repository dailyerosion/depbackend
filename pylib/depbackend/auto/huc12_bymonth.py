"""Monthly graphic available when viewing HUC12 summary."""

import calendar
from io import BytesIO

import numpy as np
import pandas as pd
from pydantic import Field
from pyiem.database import get_sqlalchemy_conn
from pyiem.exceptions import NoDataFound
from pyiem.plot import figure
from pyiem.webutil import CGIModel, iemapp

TITLES = {
    "qc_precip": "Precipitation (inch)",
    "avg_runoff": "Water Runoff (inch)",
    "avg_loss": "Soil Detachment (T/a)",
    "avg_delivery": "Hillslope Soil Delivery (T/a)",
}


class Schema(CGIModel):
    """See how we are called."""

    huc12: str = Field(
        default="070600040601",
        description="HUC12 to summarize",
        max_length=12,
    )
    scenario: int = Field(
        default=0,
        description="Scenario ID to generate metadata for",
    )


def make_plot(huc12, scenario):
    """Make the map"""
    with get_sqlalchemy_conn("idep") as conn:
        df = pd.read_sql(
            """
            SELECT extract(year from valid) as yr,
            extract(month from valid) as mo,
            sum(avg_loss) * 4.463 as avg_loss,
            sum(avg_delivery) * 4.463 as avg_delivery,
            sum(qc_precip) / 25.4 as qc_precip,
            sum(avg_runoff) / 25.4 as avg_runoff
            from results_by_huc12
            WHERE huc_12 = %s and scenario = %s GROUP by mo, yr
            """,
            conn,
            params=(huc12, scenario),
            index_col=None,
        )
    if df.empty:
        raise NoDataFound("No data found for HUC12")
    gdf = df.groupby("mo").mean()
    fig = figure(
        logo="dep",
        apctx={"_r": "43"},
        title=(
            f"Monthly Average for HUC12: {huc12} "
            f"({df['yr'].min():.0f}-{df['yr'].max():.0f}) "
        ),
    )
    boxes = [
        (0.1, 0.5, 0.4, 0.35),
        (0.6, 0.5, 0.37, 0.35),
        (0.1, 0.05, 0.4, 0.35),
        (0.6, 0.05, 0.37, 0.35),
    ]

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(
                rect.get_x() + rect.get_width() / 2.0,
                1.05 * height,
                f"{height:.1f}",
                ha="center",
                va="bottom",
            )

    for i, varname in enumerate(TITLES):
        ax = fig.add_axes(boxes[i])
        bars = ax.bar(
            gdf.index.values, gdf[varname].values, color="tan", align="center"
        )
        ax.grid(True)
        ax.set_ylabel(TITLES[varname])
        ax.set_xticks(np.arange(1, 13))
        ax.set_xticklabels(calendar.month_abbr[1:])
        ax.set_xlim(0.5, 12.5)
        ax.set_ylim(0, gdf[varname].max() * 1.15)
        ax.text(
            0,
            1.01,
            f"Sum: {gdf[varname].sum():.02f}",
            transform=ax.transAxes,
        )

        autolabel(bars)
    ram = BytesIO()
    fig.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    return ram.read()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do something fun"""
    start_response("200 OK", [("Content-type", "image/png")])
    return [make_plot(environ["huc12"], environ["scenario"])]
