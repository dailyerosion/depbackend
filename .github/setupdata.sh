#!/bin/bash
set -x
set -e

cp tests/data/097.50x035.50.cli /i/0/cli/097x035/

cp tests/data/074440.win /i/0/wind/074/

curl -o /opt/iem/data/gis/meta/5070.prj https://raw.githubusercontent.com/akrherz/iem/refs/heads/main/data/gis/meta/5070.prj
