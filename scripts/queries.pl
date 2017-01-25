#!/usr/bin/perl
use warnings;
use strict;

use DBI;
use ApacheLogDB;
use Getopt::Long;

# my $db = 'ApacheLogfile.db';
# 
# die unless -e $db;
# my $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";


Getopt::Long::GetOptions (
  "count-per-day"           => \my $count_per_day,
  "day:s"                   => \my $show_day,
  "id:i"                    => \my $show_id,
  "fqn:s"                   => \my $show_fqn,
  "order-by-count"          => \my $order_by_cnt
) or die;


# my $sth = $dbh -> prepare ("
#   select
#     count(*) cnt,
#     min(path) path_min,
#     max(path) path_max,
#     referrer
#   from
#     log
#   where
#     rogue    = 0         and
#     robot    = ''        and
#     referrer is not null and
#     referrer not like '\%renenyffenegger.ch\%' and
#     referrer like '%google%search?%'
#   group by
#     referrer
#   order by
#     count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %-50s %-50s %s\n", $r->{cnt}, $r->{path_min}, $r->{path_max}, $r->{referrer});
# }

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

if ($count_per_day) { # {

  my $order_by = "date(t, 'unixepoch')";

  if ($order_by_cnt) {
     $order_by = "count(*)";
  }

  my $sth = $dbh -> prepare ("
     select
       count(*) cnt,
       date(t, 'unixepoch') dt
     from
       log
     where
       robot = '' and
       rogue = 0
     group by
       date(t, 'unixepoch')
     order by
       $order_by
     ");
  $sth -> execute;
  while (my $r = $sth -> fetchrow_hashref) {
     printf("%6i %s\n", $r->{cnt}, $r->{dt});
  }

} # }
elsif ($show_day) { #  {

  query_flat(
      'time', 
      "date(t, 'unixepoch') = :1", $show_day
  );

} #  }
elsif ($show_fqn) { #  {

  query_flat(
     'datetime',
     "fqn  like :1", '%' . $show_fqn  . '%'
  );


} #  }
elsif ($show_id) { #  {

  my $sth = $dbh -> prepare ("
    select
      t                        t,
      datetime(t, 'unixepoch') dttm,
      method,
      path,
      status,
      referrer,
      rogue,
      requisite,
      robot,
      ipnr,
      fqn,
      agent,
      size
    from
      log
    where
      id = :1
  ");
  $sth -> execute($show_id);

  my $r = $sth->fetchrow_hashref;

  unless ($r) {
    print "No record found\n";
    exit;

  }

  print  "\n";
  printf "%s %s\n", $r->{method}, $r->{path};
  printf "  date:     %s  (%d)\n", $r->{dttm}, $r->{t};
  printf "  status:   %d\n"      , $r->{status};
  printf "  referrer: %s\n"      , $r->{referrer};
  printf "  robot:    %s\n"      , $r->{robot};
  printf "  ipnr:     %s %s\n"   , $r->{ipnr}, $r->{fqn};
  printf "  agent:    %s\n"      , $r->{agent};
  printf "  rog/req:  %d %d\n"   , $r->{rogue}, $r->{requisite};
  printf "  size:     %s\n"      , $r->{size};
  
} #  }

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


sub query_flat {

  my $tm        = shift;
  my $where     = shift;
  my $where_val = shift;


  my $sth = $dbh -> prepare ("
    select
      id,
      $tm(t, 'unixepoch') tm,
      ipnr,
      fqn,
      path,
      referrer
    from
      log
    where
      robot     = '' and
      rogue     = 0  and
      requisite = 0  and
      $where
    order by
      t
  ");

  $sth -> execute($where_val);


  while (my $r = $sth -> fetchrow_hashref) {
    my $fqn = shorten_fqn($r->{fqn}, $r->{ipnr});
    printf("%6d  %s  %-80s %-20s %s\n", $r->{id}, $r->{tm}, $r->{path}, $fqn, $r->{referrer});
  }

}
