#!/usr/bin/perl
use warnings;
use strict;

use DBI;

my $db = 'ApacheLogfile.db';

die unless -e $db;
my $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";

my $sth = $dbh -> prepare ("
  select
    count(*) cnt,
    min(path) path_min,
    max(path) path_max,
    referrer
  from
    log
  where
    rogue    = 0         and
    robot    = ''        and
    referrer is not null and
    referrer not like '\%renenyffenegger.ch\%' and
    referrer like '%google%search?%'
  group by
    referrer
  order by
    count(*)");
$sth -> execute;
while (my $r = $sth -> fetchrow_hashref) {
   printf("%6i %-50s %-50s %s\n", $r->{cnt}, $r->{path_min}, $r->{path_max}, $r->{referrer});
}

# my $sth = $dbh -> prepare ("select count(*) cnt, path from log where referrer like '\%google.\%' group by path order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{path});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, path from log group by path order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{path});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, date(t, 'unixepoch') dt from log group by date(t, 'unixepoch') order by date(t, 'unixepoch')");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{dt});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, path from log where t between strftime('\%s', ?) and strftime('\%s', ?) group by path order by count(*)");
# $sth -> execute('2016-12-06', '2016-12-07');
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{path});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, agent from log where t between strftime('\%s', ?) and strftime('\%s', ?) group by agent order by count(*)");
# $sth -> execute('2016-12-06', '2016-12-07');
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{agent});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, agent from log  group by agent order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{agent});
# }


# my $sth = $dbh -> prepare ("select
#   count(*) cnt,
#   case when robot = '' then 0 else 1 end robot,
# --robot,
#   path,
#   status
# from
#   log
# where
#   rogue  = 0 and
#   status = 404
# group by
#   robot,
# --rogue,
#   path
# order by
#   status,
#   count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %3d %d %s\n", $r->{cnt}, $r->{status}, $r->{robot}, $r->{path});
# }

# my $sth = $dbh -> prepare ("select count(*) cnt, robot from log  group by robot order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i  %s\n", $r->{cnt}, $r->{robot});
# }

# my $sth = $dbh -> prepare ("
#   select
#     count(*) cnt,
#     ipnr,
# --  path,
#     agent
#   from
#     log
#   where
#     path = '/wp-content/plugins/revslider/js/rev_admin.js'
#   group by
#     ipnr,
#     agent
#   order by
#     count(*)
# ");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%3i  %15s  %s\n", $r->{cnt}, $r->{ipnr}, $r->{agent});
# }

