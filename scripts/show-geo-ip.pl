#!/usr/bin/perl
use warnings;
use strict;

use Geo::IP;
use ApacheLogDB;

my $city   = Geo::IP->open('/usr/local/share/GeoIP/GeoIPCity.dat', GEOIP_STANDARD) or die;

my $sth = $dbh -> prepare("
  select
    datetime(t, 'unixepoch') dt,
    path,
    ipnr,
    fqn
  from
    log
  where
    rogue     = 0                   and
    robot     = ''                  and
    requisite = 0                   and
    path not like '%/notes/%'       and
    path not like '%/Biblisches/%'
  order by
   t desc limit 10000");
$sth -> execute;
while (my $r = $sth->fetchrow_hashref) {

  my $cr = $city->record_by_addr($r->{ipnr});

  my $fqn = shorten_fqn($r->{fqn}, $r->{ipnr});

  printf ("%s %-80s %-20s %s %-20s\n", $r->{dt}, $r->{path}, $fqn, $cr->{country_code3}, $cr->{city}) if $cr->{country_code3} eq 'CHE' and $fqn ne 'swisscom.ch' and $fqn ne 'hispeed.ch' and $fqn ne 'adslplus.ch' and $fqn ne 'green.ch'
}
