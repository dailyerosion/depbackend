<?php 
date_default_timezone_set('America/Chicago');
$huc_12 = isset($_REQUEST['huc12'])? $_REQUEST['huc12']: die();
$date = isset($_REQUEST['date'])? strtotime($_REQUEST['date']): die();
$date2 = isset($_REQUEST['date2'])? strtotime($_REQUEST['date2']): null;
$scenario = isset($_REQUEST["scenario"]) ? intval($_REQUEST["scenario"]): 0;
$year = date("Y", $date);

$dbconn = pg_connect("dbname=idep host=iemdb user=nobody");
function timeit($db, $name, $sql){
	$start = time();
	$rs = pg_execute($db, $name, $sql);
	$end = time();
	//echo "<br />Query time: ". ($end - $start);
	return $rs;
}

/* Find the HUC12 this location is in */
$rs = pg_prepare($dbconn, "SELECT", "SELECT hu_12_name from huc12 WHERE 
		huc_12 = $1 and scenario = $2");
$rs = timeit($dbconn, "SELECT", Array($huc_12, $scenario));
if (pg_num_rows($rs) != 1){
	echo "ERROR: No township found!";
	die();
}
$row = pg_fetch_assoc($rs,0);
$hu12name = $row["hu_12_name"];

$nicedate = date("d M Y", $date);
if ($date2 != null){
	$nicedate = sprintf("%s to %s", date("d M Y", $date), date("d M Y", $date2));
}
echo <<<EOF
<h4>Detailed Data for {$hu12name}</h4>
<form name="changer" method="GET">
<strong>HUC 12:</strong>
<input type="text" value="$huc_12" name="huc_12" size="12"/>
<button onclick="javascript: viewEvents('{$huc_12}', 'daily');" class="btn btn-sm" type="button" data-toggle="modal" data-target="#eventsModal"><i class="glyphicon glyphicon-th-list"></i> Daily</button>
<button onclick="javascript: viewEvents('{$huc_12}', 'yearly');" class="btn btn-sm" type="button" data-toggle="modal" data-target="#eventsModal"><i class="glyphicon glyphicon-th-list"></i> Yearly</button>
</form>
EOF;
/* Find the HRAP cell */
/*
 $rs = pg_prepare($dbconn, "HSELECT", "SELECT hrap_i from hrap_polygons WHERE
		ST_Within(ST_GeomFromText($1, 4326), ST_Transform(the_geom,4326))");
$rs = timeit($dbconn, "HSELECT", Array('POINT('.$lon .' '. $lat .')'));
$row = pg_fetch_assoc($rs,0);
$hrap_i = $row["hrap_i"];
*/

/* Get daily total */
/*
$rs = pg_prepare($dbconn, "RAINFALL0", "select rainfall / 25.4 as rainfall
		from daily_rainfall_$year
		WHERE valid = $1 and hrap_i = $2");
$rs = timeit($dbconn, "RAINFALL0", Array(date("Y-m-d", $date), $hrap_i));
if (pg_num_rows($rs) == 0){
	echo "<br /><strong>Rainfall:</strong> Missing";
} else{
	$row = pg_fetch_assoc($rs, 0);
	echo "<br /><strong>Rainfall:</strong> ". sprintf("%.2f in", $row["rainfall"]);
}
*/

/* Get monthly total */
/*
$rs = pg_prepare($dbconn, "RAINFALL2", "select rainfall / 25.4 as rainfall
		from monthly_rainfall_$year
		WHERE valid = $1 and hrap_i = $2");
$rs = timeit($dbconn, "RAINFALL2", Array(date("Y-m-01", $date), $hrap_i));
if (pg_num_rows($rs) == 0){
	echo "<br /><strong>Month Rainfall:</strong> Missing";
} else{
	$row = pg_fetch_assoc($rs, 0);
	echo "<br /><strong>". date("M", $date) ." Rainfall:</strong> ". sprintf("%.2f in", $row["rainfall"]);
}
*/
/* Get yearly total */
/*
$rs = pg_prepare($dbconn, "RAINFALL3", "select avg(rainfall) / 25.4 as rainfall
		from yearly_rainfall
		WHERE valid = $1 and hrap_i = $2");
$rs = timeit($dbconn, "RAINFALL3", Array(date("Y-01-01", $date), $hrap_i));
if (pg_num_rows($rs) == 0){
	echo "<br /><strong>$year Rainfall:</strong> 0.00 in";
} else{
	$row = pg_fetch_assoc($rs, 0);
	echo "<br /><strong>$year Rainfall:</strong> ". sprintf("%.2f in", $row["rainfall"]);
}
*/

/* Fetch Results 
echo "<br />--- IDEPv1 Township Summary ---";
$rs = pg_prepare($weppconn, "RES", "select * from results_by_twp WHERE
		valid = $1 and model_twp = $2 ");
$rs = timeit($weppconn, "RES", Array(date("Y-m-d", $date), $model_twp));
if (pg_num_rows($rs) == 0){
	echo "<br /><strong>No Erosion/Runoff</strong>";
} else{
	$row = pg_fetch_assoc($rs, 0);
	echo "<br /><strong>Average Rainfall:</strong> ". sprintf("%.2f in", $row["avg_precip"] / 25.4);
	echo "<br /><strong>Average Erosion:</strong> ". sprintf("%.2f T/A", $row["avg_loss"] * 4.463);
	echo "<br /><strong>Average Runoff:</strong> ". sprintf("%.2f in", $row["avg_runoff"] / 25.4);
}
*/

/* Fetch Results */
echo "<h4>{$nicedate} Summary</h4>";
$rs = pg_prepare($dbconn, "RES", "select sum(qc_precip) as qc_precip,
		sum(avg_runoff) as avg_runoff, sum(avg_loss) as avg_loss,
		sum(avg_delivery) as avg_delivery from results_by_huc12 WHERE 
		valid >= $1 and valid <= $2 and huc_12 = $3 and scenario = $4");
$rs = timeit($dbconn, "RES", Array(date("Y-m-d", $date),
		date("Y-m-d", ($date2 == null) ? $date: $date2),
		$huc_12, $scenario));
if (pg_num_rows($rs) == 0){
	$row = Array("qc_precip" => 0, 'avg_runoff' => 0,
				'avg_loss' => 0, 'avg_delivery' => 0);
} else{
	$row = pg_fetch_assoc($rs, 0);
}
echo '<table class="table table-condensed table-bordered">';
echo "<tr><th>Precipitation</th><td>". sprintf("%.2f in", $row["qc_precip"] / 25.4) ."</td></tr>";
echo "<tr><th>Runoff</th><td>". sprintf("%.2f in", $row["avg_runoff"] / 25.4) ."</td></tr>";
echo "<tr><th>Detachment</th><td>". sprintf("%.2f T/A", $row["avg_loss"] * 4.463) ."</td></tr>";
echo "<tr><th>Hillslope Soil Loss</th><td>". sprintf("%.2f T/A", $row["avg_delivery"] * 4.463) ."</td></tr>";
echo "</table>";

/* Get top events */
$rs = pg_prepare($dbconn, "TRES", "select valid from results_by_huc12 WHERE
		huc_12 = $1 and valid > '2007-01-01' and scenario =$2 and avg_loss > 0
		ORDER by avg_loss DESC LIMIT 10");
$rs = timeit($dbconn, "TRES", Array($huc_12, $scenario));
if (pg_num_rows($rs) == 0){
	echo "<br /><strong>Top events are missing!</strong>";
} else{
	echo "<h4>Top 10 Events</h4>";
	echo "<table class=\"table table-condensed table-striped table-bordered\">";
	for ($i=0;$row=@pg_fetch_assoc($rs,$i);$i++){
		$ts = strtotime($row["valid"]);
		if ($i % 2 == 0){ echo "<tr>"; }
		echo sprintf("<td><span class=\"badge\">%s</span> <a href='javascript:setDate(%s,%s,%s);'>%s</a></td>",
				$i+1, date("Y", $ts), date("m", $ts), date("d", $ts),
				date("M j, Y", $ts));
		if ($i % 2 == 1){ echo "</tr>\n"; }
	}
	echo "</table>";
}
?>
