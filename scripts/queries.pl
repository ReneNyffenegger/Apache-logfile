#!/usr/bin/perl
use warnings;
use strict;

use DBI;
use ApacheLogDB;
use Getopt::Long;

unless (@ARGV) { #_{

  usage();
  exit();

} #_}

Getopt::Long::GetOptions ( #_{
  "count-per-day"           => \my $count_per_day,
  "day:s"                   => \my $show_day,
  "fqn:s"                   => \my $show_fqn,
  "hours:i"                 => \my $hours,
  "id:i"                    => \my $show_id,
  "ips"                     => \my $show_ips,
  "ip:s"                    => \my $show_ip,
  "last-days:i"             => \my $last_days,
  "most-accessed"           => \my $most_accessed,
  "referrers"               => \my $referrers,
  "order-by-count"          => \my $order_by_cnt,
  "path:s"                  => \my $path,
  "since-last-load"         => \my $since_last_load,
  "tq-not-filtered"         => \my $tq_not_filtered,
  "what"                    => \my $what,
  "200"                     => \my $not_200
) or die; #_}

if ($what) { #_{
  usage();
  exit();
} #_}

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
elsif ($show_ips) { #_{

  my $where_def = where();

  my $sth = $dbh -> prepare ("
    select
      count(*) cnt,
      ipnr,
      gip_country,
      gip_city,
      fqn,
      min(path) min_path
    from
      log
    where
      $where_def
    group by
      ipnr,
      gip_country,
      gip_city,
      fqn
    order by
      count(*)
");
  
  $sth -> execute;

  while (my $r = $sth -> fetchrow_hashref) {
     printf("%4i %-15s %s %-20s %-30s %-70s\n", $r->{cnt}, $r->{ipnr}, $r->{gip_country}, $r->{gip_city}, fqn_($r->{fqn}), $r->{min_path});
  }
} #_}
elsif ($path) { #_{

  my $t_start = t_start();

  my $stmt = "
    select
      t,
      method,
      status,
      rogue,
      robot,
      gip_country,
      gip_city,
      ipnr,
      fqn,
      referrer,
      agent
    from
      log
    where
      t > $t_start and
      path = '$path'
    order by
      t
--    ipnr
";


  my $sth = $dbh -> prepare ($stmt);
  
  $sth -> execute;

  while (my $r = $sth -> fetchrow_hashref) {
    $r->{gip_city} = substr($r->{gip_city}, 0, 20);

    $r->{fqn} = fqn_($r->{fqn});

    if ($r->{robot}) {
      $r->{robot} = 'b';
    }
    $r->{agent}=substr($r->{agent}, 0, 110);

    $r->{t} = t_2_date_string($r->{t}); 

    printf("%s %s %d %d %1s %s %-20s %-15s %-30s %-40s %-40s\n", @$r{qw(t method status rogue robot gip_country gip_city ipnr fqn referrer agent)});
  }
} #_}
elsif ($show_ip) { #_{

  my $t_last_load = t_last_load();

  my $stmt = "
    select
      t,
      method,
      status,
      rogue,
      robot,
      path,
      referrer,
      agent
    from
      log
    where
      t > $t_last_load and
      requisite = 0 and
      ipnr = '$show_ip'
    order by
      t
";


  my $sth = $dbh -> prepare ($stmt);
  
  $sth -> execute;

  my $rno = 0;
  while (my $r = $sth -> fetchrow_hashref) {
    $rno ++;

    if ($rno == 1) {
      print "Agent: $r->{agent}\n";
    }

    $r->{t} = t_2_date_string($r->{t}); 

    printf("%s %s %d %d %s %-70s %-70s\n", @$r{qw(t method status rogue robot path referrer)});
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
      $where_def                                                                    and
      referrer <> '-'                                                               and
      referrer not like 'http://renenyffenegger.%'                                  and
      referrer not like 'http://www.renenyffenegger.ch%'                            and
      referrer not like 'http://www.adp-gmbh.ch%'                                   and
      referrer not like 'http://adp-gmbh.ch%'                                       and
      referrer not like 'http://www.google.%'                                       and
      referrer not like 'https://www.google.%'                                      and
      referrer not like 'http://yandex.ru/%'                                        and
      referrer not like 'https://t.co/%'                                            and
      referrer not like 'https://translate.googleusercontent.com/%'                 and
      referrer !=       'android-app://com.google.android.googlequicksearchbox'     and
      referrer !=       '(null)'
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
elsif ($not_200) { #_{
  
  my $where_def = where();

  # Status 304: Not modified

  my $sth = $dbh -> prepare ("
    select
      count(*) cnt,
      status,
      path,
      referrer
    from
      log
    where
      $where_def and
      status not in (200, 304)
    group by
      status,
      path,
      referrer
    order by
      cnt
");
  
  $sth -> execute;

  while (my $r = $sth -> fetchrow_hashref) {
     printf("%6i %3i %-90s %s\n", $r->{cnt}, $r->{status}, substr($r->{path}, 0, 90), $r->{referrer});
  }
} #_}
elsif ($hours) { #_{

  my $t_ = t_now() - 60*60 * $hours;

  query_flat(
      'datetime', 
      "t>=:1",  $t_
  );

} #_}
elsif ($last_days) { #_{

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
elsif ($since_last_load) { #_{

  query_flat(
      'datetime', 
      "'a' =:1", 'a'
  );

  
} #_}


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


sub query_flat { #_{

  my $tm        = shift;
  my $where_add = shift;
  my $where_val = shift;

  my $where_def = where();

  my $stmt  = "
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
  ";

  print $stmt;

  my $sth = $dbh -> prepare ($stmt);

  $sth -> execute($where_val);


  while (my $r = $sth -> fetchrow_hashref) {
    my $fqn = shorten_fqn($r->{fqn}, $r->{ipnr});
    printf("%6d  %s  %-80s %-20s %s %-20s %s\n", $r->{id}, $r->{tm}, $r->{path}, $fqn, $r->{gip_country}, $r->{gip_city}, $r->{referrer});
  }

} #_}

sub where { #_{

  my $where_agent = '1=1';
     $where_agent = "agent != 'Mozilla/5.0 (TQ)'" unless $tq_not_filtered;


  my $where = "
    robot     = ''      and
    rogue     =  0      and
    requisite =  0      and
    $where_agent ";

# my $t_start = 0;
  my $t_start = t_start();

# if ($last_days) { #_{
#   $t_start = t_now() - 60*60 * 24* $last_days;
# } #_}
# else {
#   $t_start = t_last_load();
# }


  $where .= "  and t > $t_start ";
  return $where;

} #_}

sub t_start { #_{

  if ($last_days) { #_{
    return t_now() - 60*60 * 24* $last_days;
  } #_}
  else {
    return t_last_load();
  }

} #_}

sub t_last_load { #_{
  return ($dbh->selectrow_array('select max(max_t_at_load_start) from load_hist'))[0];
} #_}

sub fqn_ {
  my $fqn = shift;

  my @fqn_parts = reverse split '\.', $fqn;
  if (@fqn_parts > 1) {
    if (grep {$_ eq $fqn_parts[0]} qw(tr br uk cn)) {
      $fqn = "$fqn_parts[2].$fqn_parts[1].$fqn_parts[0]";
    }
    else {
      $fqn = "$fqn_parts[1].$fqn_parts[0]";
    }
  }

  return substr($fqn, 0, 30);
}

sub usage { #_{

  print "
  --count-per-day     [ --order-by-count ]
  --day:s
  --fqn:s
  --hours:i
  --id:i
  --ips              count per IP
  --ip:s             Show visits of ipnr
  --id:i
  --last-days:i
  --most-accessed
  --path             /foo/bar
  --referrers
  --since-last-load
  --tq-not-filtered
  --what
  --200
";
} #_}
