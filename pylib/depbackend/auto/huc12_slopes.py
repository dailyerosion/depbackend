"""Mapping Interface"""

import glob
import os
from io import BytesIO

import numpy as np
import seaborn as sns
from pydantic import Field
from pydep.io.wepp import read_slp
from pyiem.exceptions import NoDataFound
from pyiem.webutil import CGIModel, iemapp


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


def make_plot(huc12: str, scenario: int):
    """Make the map"""
    mydir = f"/i/{scenario}/slp/{huc12[:8]}/{huc12[8:]}"
    if not os.path.isdir(mydir):
        raise NoDataFound("No data found for this scenario")
    os.chdir(mydir)
    res = []
    for fn in glob.glob("*.slp"):
        try:
            slp = read_slp(fn)
        except Exception:
            continue
        bulk = (slp[-1]["y"][-1]) / slp[-1]["x"][-1]
        length = slp[-1]["x"][-1]
        if bulk < -1:
            continue
        res.append([(0 - bulk) * 100.0, length])

    data = np.array(res)
    g = sns.jointplot(
        x=data[:, 1], y=data[:, 0], s=40, zorder=1, color="tan"
    ).plot_joint(sns.kdeplot, n_levels=6)
    g.ax_joint.set_xlabel("Slope Length [m]")
    g.ax_joint.set_ylabel("Bulk Slope [%]")
    g.figure.subplots_adjust(top=0.8, bottom=0.2, left=0.15)
    g.ax_joint.grid()
    g.ax_marg_x.set_title(
        (
            f"HUC12 {huc12} DEP Hillslope\n"
            "Kernel Density Estimate (KDE) Overlain"
        ),
        fontsize=10,
    )

    ram = BytesIO()
    g.figure.set_size_inches(3.6, 2.4)
    g.figure.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    return ram


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do something fun"""
    start_response("200 OK", [("Content-type", "image/png")])
    return [make_plot(environ["huc12"], environ["scenario"]).read()]
