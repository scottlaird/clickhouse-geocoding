-- Basic config for geocoding IP addresses in Clickhouse using
-- city-level data from DB-IP.  See
-- https://scottstuff.net/posts/2025/03/21/geocoding-ip-addresses-with-clickhouse/
-- for discussion.  This is based on work from Clickhouse
-- (https://clickhouse.com/blog/geolocating-ips-in-clickhouse-and-grafana),
-- Guillaume Matheron
-- (https://blog.guillaumematheron.fr/2023/486/ip-based-geolocation-in-clickhouse-with-ipv6/),
-- and a few of the commenters on Guillaume's blog post.


-- Fetch the free public version of DB-IP's IPv4 geocoding data.  Note
-- that this is licensed with CC_BY_4.0 and requires attribution for
-- use.  See
-- https://github.com/sapics/ip-location-db/tree/main/dbip-city
--
-- This table is backed by the CSV URL provided; every read from this
-- table will result in a new fetch over HTTP, which is great for
-- freshness but lousy for performance.
CREATE OR REPLACE TABLE geoip_url4(
    `ip_range_start` IPv4, 
    `ip_range_end` IPv4, 
    `country_code` Nullable(String), 
    `state1` Nullable(String), 
    `state2` Nullable(String), 
    `city` Nullable(String), 
    `postcode` Nullable(String), 
    `latitude` Float64, 
    `longitude` Float64, 
    `timezone` Nullable(String)
) engine=URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-city/dbip-city-ipv4.csv.gz', 'CSV');

-- Fetch the free public version of DB-IP's IPv6 geocoding data.  Note
-- that this is licensed with CC_BY_4.0 and requires attribution for
-- use.  See
-- https://github.com/sapics/ip-location-db/tree/main/dbip-city
CREATE OR REPLACE TABLE geoip_url6 (
    `ip_range_start` IPv6,
    `ip_range_end` IPv6,
    `country_code` Nullable(String),
    `state1` Nullable(String),
    `state2` Nullable(String),
    `city` Nullable(String),
    `postcode` Nullable(String),
    `latitude` Float64,
    `longitude` Float64,
    `timezone` Nullable(String)
)
ENGINE = URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-city/dbip-city-ipv6.csv.gz', 'CSV');

-- Create a table for holding geo-mapping data with CIDR-format addresses
-- instead of (first, last) IP ranges.  This table will hold both IPv4 and
-- IPv6 data
CREATE OR REPLACE TABLE geoip (
   `cidr` String,
   `latitude` Float64,
   `longitude` Float64,
   `country_code` String,
   `state1` String,
   `state2` String,
   `city` String
) 
engine = MergeTree() 
order by cidr;

-- Create a materialized view that turns the two `geoip_url*` tables
-- (above) into a single view, replacing the source tables' start/end
-- addressing with CIDR.
--
-- Note that this is rebuilt ~weekly automatically.
CREATE MATERIALIZED VIEW geocode_mv
REFRESH AFTER 1 week RANDOMIZE FOR 1 day
TO geoip AS
  WITH
      bitXor(ip_range_start, ip_range_end) as xor,
      ceil(log2(xor+1)) as unmatched,
      32 - unmatched as cidr_suffix,
      toIPv4(bitAnd(bitNot(pow(2, unmatched) - 1), ip_range_start)::UInt64) as cidr_address
  SELECT
      concat(toString(cidr_address),'/',toString(cidr_suffix)) as cidr,
      latitude,
      longitude,
      country_code,
      state1,
      state2,
      city
  FROM
      geoip_url4
UNION ALL
  WITH
      bitXor(ip_range_start, ip_range_end) as xor,
      ceil(log2(xor+1)) as unmatched,
      128 - unmatched as cidr_suffix,
      IPv6NumToString(toFixedString(unbin(rightPad(substr(bin(ip_range_start), 1, cidr_suffix), 128, '0')), 16)) as cidr_address
  SELECT
      concat(toString(cidr_address),'/',toString(cidr_suffix)) as cidr,
      latitude,
      longitude,
      country_code,
      state1,
      state2,
      city
  FROM
    geoip_url6;

-- Create a dictionary for fast lookups of both IPv4 and IPv6
-- addresses, based on the unified view from above.
CREATE OR REPLACE DICTIONARY ip_trie (
   `cidr` String,
   `latitude` Float64,
   `longitude` Float64,
   `country_code` String,
   `state1` String,
   `state2` String,
   `city` String
) 
PRIMARY KEY cidr
SOURCE(clickhouse(table ‘geoip’))
LAYOUT(ip_trie)
LIFETIME(86400);  -- update daily


-- To use, see the blog post at the top, or
--
--   SELECT ip, dictGet('ip_trie', 'country_code', toIPv6(ip)) AS country FROM ...
--
-- to start.
