"""Mapping Interface"""

import glob
import os
from io import BytesIO

import numpy as np
import seaborn as sns
from paste.request import parse_formvars
from pydep.io.wepp import read_slp
from pyiem.plot.use_agg import plt


def make_plot(huc12, scenario):
    """Make the map"""
    os.chdir("/i/%s/slp/%s/%s" % (scenario, huc12[:8], huc12[8:]))
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
    g.fig.subplots_adjust(top=0.8, bottom=0.2, left=0.15)
    g.ax_joint.grid()
    g.ax_marg_x.set_title(
        ("HUC12 %s DEP Hillslope\n" "Kernel Density Estimate (KDE) Overlain")
        % (huc12,),
        fontsize=10,
    )

    ram = BytesIO()
    plt.gcf().set_size_inches(3.6, 2.4)
    plt.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    return ram.read()


def application(environ, start_response):
    """Do something fun"""
    form = parse_formvars(environ)
    huc12 = form.get("huc12", "000000000000")[:12]
    scenario = int(form.get("scenario", 0))

    start_response("200 OK", [("Content-type", "image/png")])
    return [make_plot(huc12, scenario)]
