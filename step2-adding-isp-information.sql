-- Extends IP information by adding network information, with AS number and organization.

-- Fetch the free public version of DB-IP's IPv4 ASN data.  Note
-- that this is licensed with CC_BY_4.0 and requires attribution for
-- use.  See
-- https://github.com/sapics/ip-location-db/tree/main/dbip-asn
--
-- This table is backed by the CSV URL provided; every read from this
-- table will result in a new fetch over HTTP, which is great for
-- freshness but lousy for performance.
CREATE OR REPLACE TABLE geoip_asn_url4(
    `ip_range_start` IPv4, 
    `ip_range_end` IPv4,
    `as_number` Int,
    `as_organization` Nullable(String),
) engine=URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-asn/dbip-asn-ipv4.csv', 'CSV');

-- Fetch the free public version of DB-IP's IPv6 ASN data.  Note
-- that this is licensed with CC_BY_4.0 and requires attribution for
-- use.  See
-- https://github.com/sapics/ip-location-db/tree/main/dbip-asn
CREATE OR REPLACE TABLE geoip_asn_url6 (
    `ip_range_start` IPv6, 
    `ip_range_end` IPv6,
    `as_number` Int,
    `as_organization` Nullable(String),
) engine=URL('https://raw.githubusercontent.com/sapics/ip-location-db/master/dbip-asn/dbip-asn-ipv6.csv', 'CSV');


-- Create a table for holding ASN data with CIDR-format addresses
-- instead of (first, last) IP ranges.  This table will hold both IPv4 and
-- IPv6 data
CREATE OR REPLACE TABLE geoip_asn (
   `cidr` String,
   `as_number` Int,
   `as_organization` Nullable(String),
) 
engine = MergeTree() 
order by cidr;

-- Create a materialized view that turns the two `geoip_asn_url*` tables
-- (above) into a single view, replacing the source tables' start/end
-- addressing with CIDR.
--
-- Note that this is rebuilt ~weekly automatically.
CREATE MATERIALIZED VIEW geoip_asn_mv
REFRESH EVERY 1 week RANDOMIZE FOR 1 day
TO geoip_asn AS
  WITH
      bitXor(ip_range_start, ip_range_end) as xor,
      ceil(log2(xor+1)) as unmatched,
      32 - unmatched as cidr_suffix,
      toIPv4(bitAnd(bitNot(pow(2, unmatched) - 1), ip_range_start)::UInt64) as cidr_address
  SELECT
      concat(toString(cidr_address),'/',toString(cidr_suffix)) as cidr,
      as_number,
      as_organization
  FROM
      geoip_asn_url4
UNION ALL
  WITH
      bitXor(ip_range_start, ip_range_end) as xor,
      ceil(log2(xor+1)) as unmatched,
      128 - unmatched as cidr_suffix,
      IPv6NumToString(toFixedString(unbin(rightPad(substr(bin(ip_range_start), 1, cidr_suffix), 128, '0')), 16)) as cidr_address
  SELECT
      concat(toString(cidr_address),'/',toString(cidr_suffix)) as cidr,
      as_number,
      as_organization
  FROM
    geoip_asn_url6;

-- Create a dictionary for fast lookups of both IPv4 and IPv6
-- addresses, based on the unified view from above.
CREATE OR REPLACE DICTIONARY ip_asn_trie (
   `cidr` String,
   `as_number` Int,
   `as_organization` String
) 
PRIMARY KEY cidr
SOURCE(clickhouse(table ‘geoip_asn’))
LAYOUT(ip_trie)
LIFETIME(86400);  -- update daily


-- To use, see the blog post at the top, or
--
--   SELECT ip, dictGet('ip_asn_trie', 'as_organization', toIPv6(ip)) AS as_organization FROM ...
--
-- to start.
