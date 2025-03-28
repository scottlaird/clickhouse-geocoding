# Clickhouse Geocoding

This is a collection of SQL for implementing
[geocoding](https://en.wikipedia.org/wiki/Internet_geolocation) in
[Clickhouse](https://clickhouse.com/).  It's effectively a
version-controlled (and probably extended) copy of the code from my
blog post [Geocoding IP Addresses with
Clickhouse](https://scottstuff.net/posts/2025/03/21/geocoding-ip-addresses-with-clickhouse/)

## Adding to Clickhouse

To set this up in the first place, apply the SQL from
`step-1-basic-geocoding.sql` to Clickhouse.  It will create 3 tables,
one materialized view, and one dictionary.

If you need to update this for any reason, then you'll probably need
to manually drop the dictionary and the materialized view, as
Clickhouse doesn't currently have `CREATE OR REPLACE` for either.

On my machine, it takes up to 30 seconds to create the `ip_trie`
dictionary on first use, but after that updates shouldn't add
substantial latency.  The underlying geocoding data gets updates
monthly, and this will automatically update its view of the data
roughly once per week.

## Usage

See [Geocoding IP Addresses with
Clickhouse](https://scottstuff.net/posts/2025/03/21/geocoding-ip-addresses-with-clickhouse/)
for more detail, but here's the short version

```sql
SELECT
  ip,
  dictGet('ip_trie', 'country_code', toIPv6(ip)) AS country
FROM ...
```

You can fetch multiple fields from the `ip_trie` dictionary in a
single lookup; see the blog post for syntax.

## Attribution

The data that this uses is freely available, but is licensed by
[DB-IP](https://db-ip.com) and requires attribution for use. See
https://github.com/sapics/ip-location-db/tree/main/dbip-city for
details.
