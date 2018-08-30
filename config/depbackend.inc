# Apache Include for common dailyerosion.org stuff
  DocumentRoot /opt/depbackend/htdocs
  ServerAdmin akrherz@iastate.edu

  # RewriteEngine is not enabled for vhosts by default
  RewriteEngine On

  Alias /static/ "/mnt/idep/2/data/static/"
  <Directory "/mnt/idep/2/data/static/">
  	Options Indexes MultiViews
  </Directory>

  Alias /usage/ "/mesonet/share/usage/idep.agron.iastate.edu/"
  <Directory "/mesonet/share/usage/idep.agron.iastate.edu/">
    Options Indexes MultiViews
    AllowOverride None
    Order allow,deny
    Allow from all
  </Directory>

  <Directory "/opt/depbackend/htdocs">
    Options Indexes MultiViews FollowSymLinks
    AllowOverride All

    AddHandler wsgi-script .wsgi
    AddHandler cgi-script .py
    Options +ExecCGI
  </Directory>

  <Directory "/opt/depbackend/htdocs/auto">

    RewriteRule ^([0-9]{4})([01][0-9])([0-3][0-9])_([0-9]{4})([01][0-9])([0-3][0-9])_([0-9]+)_(.*).png$ map.wsgi?year=$1&month=$2&day=$3&year2=$4&month2=$5&day2=$6&scenario=$7&v=$8
    RewriteRule ^huc([0-9]{8,12})_([0-9]{4})([01][0-9])([0-3][0-9])_([0-9]{4})([01][0-9])([0-3][0-9])_([0-9]+)_(.*).png$ map.wsgi?year=$2&month=$3&day=$4&year2=$5&month2=$6&day2=$7&scenario=$8&v=$9&huc=$1
    RewriteRule ^([0-9]{8,12})_report.pdf$ huc12report.py?huc=$1
  </Directory>

  <Directory "/opt/depbackend/htdocs/geojson">
    SetOutputFilter DEFLATE
  </Directory>

  Alias "/admin" "/opt/depbackend/admin"
  <Directory "/opt/depbackend/admin">
        AuthUserFile /opt/iem/config/admin.users
        AuthGroupFile /dev/null
        AuthName Administrative
        AuthType Basic

        order deny,allow
        require user dep
    
    Options Indexes MultiViews FollowSymLinks
    AllowOverride All
  </Directory>

  <Directory "/opt/depbackend/admin/plots">
    RewriteRule "^slopes([0-9]+)_([0-9]{12})_([0-9A-Z]+).png$" "slopes.py?scenario=$1&huc_12=$2&model_twp=$3"
  </Directory>


  # Since RHEL Apache does not log 404s, sigh
  ErrorDocument 404 /http_errors/error404.php

  <Location "/http_errors/">
    # https://bz.apache.org/bugzilla/show_bug.cgi?id=55415
    ProxyErrorOverride Off
  </Location>

  # http://enable-cors.org/server_apache.html
  Header set Access-Control-Allow-Origin "*"
