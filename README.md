HiveUDFs
========

My Personal Collection of Hive UDFs

## Compiling

This is a Maven project. To compile it

    mvn install

## Function Lists

Below are function list that is currently in this project. More to come!

### LongToIP
**LongToIP** translates IP in long format to string format.

Usage:

    ADD JAR HiveUDFs.jar;
    CREATE TEMPORARY FUNCTION longtoip as 'net.petrabarus.hiveudfs.LongToIP';
    SELECT longtopip(2130706433) FROM table;

### IPToLong

**IPToLong** translates IP in string format to long format.

Usage:

    ADD JAR HiveUDFs.jar;
    CREATE TEMPORARY FUNCTION iptolong as 'net.petrabarus.hiveudfs.IPToLong';
    SELECT iptolong("127.0.0.1") FROM table;

### GeoIP

**GeoIP** wraps MaxMind GeoIP function for Hive. 
This is a derivation from @edwardcapriolo [hive-geoip](http://github.com/edwardcapriolo/hive-geoip).
Separate GeoIP database will be needed to run the function.
The GeoIP will need three argument.

1. IP address in long
2. IP attribute (e.g. COUNTRY, CITY, REGION, etc. See full list in the javadoc.)
3. Database file name

A lite version of the MaxMind GeoIP can be obtained from [here] (http://dev.maxmind.com/geoip/geolite).

Usage:

    ADD JAR HiveUDFs.jar;
    ADD FILE /usr/share/GeoIP/GeoIPCity.dat;
    CREATE TEMPORARY FUNCTION geoip as 'net.petrabarus.hiveudfs.GeoIP';
    SELECT GeoIP(cast(ip AS bigint), 'CITY', './GeoIPCity.dat') FROM table;

### SearchEngineKeyword

**SearchEngineKeyword** is a simple function to extract keyword from URL referrer
that comes from Google, Bing, and Yahoo. Need to expand this to cover more search
engines.

Usage

    ADD JAR HiveUDFs.jar
    CREATE TEMPORARY FUNCTION searchenginekeyword as 'net.petrabarus.hiveudfs.SearchEngineKeyword';
    SELECT searchenginekeyword(url) FROM table;

### UCWords

**UCWords** is UDF function equivalent to PHP ucwords().

Usage

    ADD JAR HiveUDFs.jar
    CREATE TEMPORARY FUNCTION ucwords as 'net.petrabarus.hiveudfs.UCWords';
    SELECT ucwords(text) FROM table;

##Copyright and License

MaxMind GeoIP is a trademark of MaxMind LLC.