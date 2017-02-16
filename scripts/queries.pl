#!/usr/bin/perl
use warnings;
use strict;

use DBI;
use ApacheLogDB;
use Getopt::Long;


Getopt::Long::GetOptions ( #_{
  "count-per-day"           => \my $count_per_day,
  "day:s"                   => \my $show_day,
  "fqn:s"                   => \my $show_fqn,
  "hours:i"                 => \my $hours,
  "id:i"                    => \my $show_id,
  "last-days:i"             => \my $last_days,
  "most-accessed"           => \my $most_accessed,
  "referrers"               => \my $referrers,
  "order-by-count"          => \my $order_by_cnt,
  "tq-not-filtered"         => \my $tq_not_filtered,
  "what"                    => \my $what
) or die; #_}

if ($what) {
  usage();
  exit();
}

if ($count_per_day) { #_{

  my $order_by = "date(t, 'unixepoch')";

  if ($order_by_cnt) {
     $order_by = "count(*)";
  }

  my $where_def = where();

  my $sth = $dbh -> prepare ("
     select
       count(*) cnt,
       date(t, 'unixepoch') dt
     from
       log
     where
       $where_def
     group by
       date(t, 'unixepoch')
     order by
       $order_by
     ");
  $sth -> execute;
  while (my $r = $sth -> fetchrow_hashref) {
     printf("%6i %s\n", $r->{cnt}, $r->{dt});
  }

} #_}
elsif ($most_accessed) { #_{
  
  my $where_def = where();

  my $sth = $dbh -> prepare ("
    select
      count(*)  cnt,
      path
    from
      log
    where
      $where_def
    group by
      path
    order by
      cnt
");
  
  $sth -> execute;

  while (my $r = $sth -> fetchrow_hashref) {
     printf("%6i %s\n", $r->{cnt}, $r->{path});
  }
} #_}
elsif ($referrers) { #_{

  my $where_def = where();

  my $path_width     = 70;
  my $referrer_width = 90;

  my $sth = $dbh-> prepare ("
    select
      count(*)   cnt,
      min(path)  path_min,
      max(path)  path_max,
      referrer
    from
      log
    where
      $where_def                                           and
      referrer <> '-'                                      and
      referrer not like 'http://renenyffenegger.%'         and
      referrer not like 'http://www.renenyffenegger.ch%'   and
      referrer not like 'http://www.adp-gmbh.ch%'          and
      referrer not like 'http://adp-gmbh.ch%'              and
      referrer not like 'http://www.google.%'              and
      referrer not like 'https://www.google.%'             and
      referrer not like 'http://yandex.ru/%'               and
      referrer not like 'https://t.co/%'                   and
      referrer not like 'https://translate.googleusercontent.com/%'
    group by
      referrer
    order by
      count(*)
  ");

  $sth -> execute;

  while (my $r = $sth -> fetchrow_hashref) {
    printf("%6i %-${path_width}s %-${path_width}s %s\n", $r->{cnt}, substr($r->{path_min}, 0, $path_width), substr($r->{path_max}, 0, $path_width), substr($r->{referrer}, 0, $referrer_width));
  }

} #_}
elsif ($show_day) { #_{

  query_flat(
      'time', 
      "date(t, 'unixepoch') = :1", $show_day
  );

} #_}
elsif ($hours) { #_{

  my $t_ = t_now() - 60*60 * $hours;

  query_flat(
      'datetime', 
      "t>=:1",  $t_
  );

} #_}
elsif ($last_days) { #_{

# my $t_ = t_now() - 60*60 * 24* $last_days;

  query_flat(
      'datetime', 
      "'a'=:1", 'a'
  );

} #_}
elsif ($show_fqn) { #_{

  query_flat(
     'datetime',
     "fqn  like :1", '%' . $show_fqn  . '%'
  );


} #_}
elsif ($show_id) { #_{

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
      gip_country,
      gip_city,
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
  printf "  Geo::IP:  %s %s\n"   , $r->{gip_country}, $r->{gip_city};
  printf "  ipnr:     %s %s\n"   , $r->{ipnr}, $r->{fqn};
  printf "  agent:    %s\n"      , $r->{agent};
  printf "  rog/req:  %d %d\n"   , $r->{rogue}, $r->{requisite};
  printf "  size:     %s\n"      , $r->{size};
  
} #_}

# my $sth = $dbh -> prepare ("select count(*) cnt, path from log where t between strftime('\%s', ?) and strftime('\%s', ?) group by path order by count(*)");
# $sth -> execute('2016-12-06', '2016-12-07');
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{path});
#_}

# my $sth = $dbh -> prepare ("select count(*) cnt, agent from log where t between strftime('\%s', ?) and strftime('\%s', ?) group by agent order by count(*)");
# $sth -> execute('2016-12-06', '2016-12-07');
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{agent});
#_}

# my $sth = $dbh -> prepare ("select count(*) cnt, agent from log  group by agent order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i %s\n", $r->{cnt}, $r->{agent});
#_}


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
#_}

# my $sth = $dbh -> prepare ("select count(*) cnt, robot from log  group by robot order by count(*)");
# $sth -> execute;
# while (my $r = $sth -> fetchrow_hashref) {
#    printf("%6i  %s\n", $r->{cnt}, $r->{robot});
#_}

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
#_}


sub query_flat {

  my $tm        = shift;
  my $where_add = shift;
  my $where_val = shift;

  my $where_def = where();


  my $sth = $dbh -> prepare ("
    select
      id,
      $tm(t, 'unixepoch') tm,
      gip_country,
      gip_city,
      ipnr,
      fqn,
      path,
      referrer
    from
      log
    where
      $where_def and
      $where_add
    order by
      t
  ");

  $sth -> execute($where_val);


  while (my $r = $sth -> fetchrow_hashref) {
    my $fqn = shorten_fqn($r->{fqn}, $r->{ipnr});
    printf("%6d  %s  %-80s %-20s %s %-20s %s\n", $r->{id}, $r->{tm}, $r->{path}, $fqn, $r->{gip_country}, $r->{gip_city}, $r->{referrer});
  }

}

sub where {

  my $where_agent = '1=1';
     $where_agent = "agent != 'Mozilla/5.0 (TQ)'" unless $tq_not_filtered;


  my $where = "
   robot     = ''      and
   rogue     =  0      and
   requisite =  0      and
   $where_agent ";

   if ($last_days) {
      my $t_ = t_now() - 60*60 * 24* $last_days;
      $where .= "  and t > $t_ "
   }

  return $where;

}

sub usage {

  print "
  --count-per-day     [ --order-by-count ]
  --day:s
  --fqn:s
  --hours:i
  --id:i
  --last-days:i
  --most-accessed
  --referrers
  --tq-not-filtered
  --what
";
}
