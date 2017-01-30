#!/usr/bin/perl
use warnings;
use strict;

use Geo::IP;

use lib '..'; use ApacheLogDB;

my $geoip   = Geo::IP->open('/usr/local/share/GeoIP/GeoIPCity.dat', GEOIP_STANDARD) or die;

my $sth_upd = $dbh -> prepare('update log set gip_country = ?, gip_city = ? where id = ?');
my @rows = @{$dbh -> selectall_arrayref('select id, ipnr from log')};

for my $row (@rows) {

  my $id   = $row->[0];
  my $ipnr = $row->[1];
  my $rec  = $geoip->record_by_addr($ipnr);
  my $ctry = $rec->{country_code};
  my $city = $rec->{city};
  print "$id $ctry $city\n";
  $sth_upd -> execute ($ctry, $city, $id);
}

$dbh->commit;
