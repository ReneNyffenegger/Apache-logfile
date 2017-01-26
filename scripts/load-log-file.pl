#!/usr/bin/perl
use warnings;
use strict;
use Time::HiRes qw(time);
use Time::Piece;
use HTTP::BrowserDetect;
use Net::DNS::Resolver;

use ApacheLogDB;


my $t_1970 = Time::Piece->strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S');

my $parse_robot_from_agent_string = 1;

my $dns_resolver = new Net::DNS::Resolver;
my %ipnrs;

my $t_max_in_log = ($dbh->selectrow_array('select max(t) from log'))[0];
print "Max t in log: $t_max_in_log\n";

my $rec_cnt = 0;

# my $insert_sth = $dbh->prepare("insert into log (t, method, path, status, referrer, requisite, rogue, robot, ipnr, fqn, agent, size  )values (strftime('\%s', ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") or die;
my   $insert_sth = $dbh->prepare("insert into log (t, method, path, status, referrer, requisite, rogue, robot, ipnr, fqn, agent, size  )values (                ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") or die;

my $already_inserted_sth = $dbh->prepare('select count(*) cnt from log where t = :1 and ipnr = :2 and path = :3 and method = :4') or die;

my $start_t = time;

print STDERR "Warning, no files specified\n" unless @ARGV;

while (my $log_f = shift @ARGV) {
  load_log_file($log_f);
}

my $end_t = time;
printf("loaded %i records in %5.2f seconds (%7.2f recs/s)\n", $rec_cnt, $end_t - $start_t, $rec_cnt/($end_t - $start_t));

$dbh -> commit;

sub load_log_file {
  my $log_f = shift;
  print "Loading $log_f\n";

  open (my $log_h, '<', $log_f) or die "could not open $log_f";

  while (my $log_l = <$log_h>) {

    my $do_insert = 1;


    my $rogue = 0;
    my $robot = '';
  
    if ( my ($year, $month, $day, $hour, $min, $sec, $ipnr, $method, $path, $http, $status, $size, $referrer, $agent) = $log_l =~ m!^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d) (\d+\.\d+\.\d+\.\d+) "(\w+) (.*) (HTTP/1\.\d)" (\d+) (\d+) "([^"]*)" "([^"]*)"!) {

      my $method_;
      if ($method eq 'GET') {
        $method_ = 'G';
      }
      elsif ($method eq 'POST') {
        $method_ = 'P';
      }
      elsif ($method eq 'HEAD') {
        $method_ = 'H';
      }
      elsif ($method eq 'PUT') {
        $method_ = 'p';
      }
      else {
        die "method = $method"
      }


      my $t_line = Time::Piece->strptime("$year-$month-$day $hour:$min:$sec", '%Y-%m-%d %H:%M:%S');
      
      my $t =$t_line - $t_1970; 

      $already_inserted_sth -> execute($t, $ipnr, $path, $method_);
      my $cnt = ($already_inserted_sth -> fetchrow_array)[0];

      if ($t > $t_max_in_log) {

        # if $t > $t_max_in_log then we should find no
        # record in the db.
        # If we find one, something has gone terribly wrong, let's
        # die then:

        die (" t > t_max_in_log %10d  %1d  %-90s %s\n", $t, $cnt, $path, $ipnr) if $cnt > 0;
      }
      else {

        # if $t <= $t_max_in_log we check if there is a missing record in
        # the db.

        if ($cnt == 0) {
        #
        # Yes, this record seems to be missing
          printf (" t[$t] <= t_max_in_log[$t_max_in_log] %10d  %1d  %-90s %s\n", $t, $cnt, $path, $ipnr);
        }
        else {
        #
        # This record was already inserted, as expected
        # We can skip the insert:
          $do_insert = 0;
        }

      }

      if ($do_insert) {

        my $fqn = ipnr_2_fqn($ipnr);

        if ($parse_robot_from_agent_string) {
          if ($fqn eq 'spider.tiger.ch.') {
            $robot = 'spider.tiger.ch';
          }
          else {
            my $ua = new HTTP::BrowserDetect($agent);
            $robot = $ua->robot_string || '';
          }
        }
  
        $rogue = is_rogue($path);


        my $requisite = 0;
        if ($path eq '/favicon.ico' or $path eq '/robots.txt' or $path =~ /\.(png|css|png|jpg|woff|js|gif|php|pdf|ttf)$/ or $path eq '/font/cartogothicstd-book-webfont.eot?') {
          $requisite = 1;
        }

      
        $insert_sth -> execute($t, $method_, $path, int($status), $referrer, $requisite, $rogue, $robot, $ipnr, $fqn, $agent, $size);
        $rec_cnt++;
      }
    }
    else {
      die "Could not parse\n$log_l\n";
    }
  
  }

}
sub is_rogue {
  my $path     = shift;
  my $referrer = shift;

  return 1 if $path =~ m!/[Ff]?ckeditor!;
  return 1 if $path =~ m!/typo3/!;
  return 1 if $path =~ m!^/wordpress/!;
  return 1 if $path =~ m!^/scripts/!;
  return 1 if $path =~ m!^/plugins/!;
  return 1 if $path =~ m!^/assets/!;
  return 1 if $path =~ m!^/assetmanager/!;
  return 1 if $path =~ m!^/joomla!;
  return 1 if $path =~ m!/editor.*/editor/!;
  return 1 if $path =~ m!^/editor/!;
  return 1 if $path =~ m!/wp-!;
  return 1 if $path =~ m!^/admin/.*\.sql$!;
  return 1 if $path =~ m!^/license\.!;
  return 1 if $path =~ m!^/cleaner[^.]*\.sh$!;
  return 1 if $path =~ m!^/admin!;
  return 1 if $path =~ m!^/xmlrpc.php!;
  return 1 if $path =~ m!^/kcfinder/!;
  return 1 if $path =~ m!^/tiki!;
  return 1 if $path eq  '/wp/';
  return 1 if uc($path) eq  '/README.txt';
  return 1 if length($path) > 500;

  $referrer = '' unless $referrer;

  return 1 if $referrer eq 'http://buttons-for-website.com'                                                                and $path eq '/';
  return 1 if $referrer eq 'http://buttons-for-your-website.com'                                                           and $path eq '/';
  return 1 if $referrer eq 'http://burger-imperia.com/'                                                                    and $path eq '/';
  return 1 if $referrer eq 'http://1-free-share-buttons.com'                                                               and $path eq '/';
  return 1 if $referrer eq 'http://blog.societyforexcellenceineducation.org/cat-31/india-dissertation-help-writing.html'   and $path eq '/';

  return 0;
}

sub ipnr_2_fqn {
  my $ipnr = shift;

  if (exists $ipnrs{$ipnr}) {
    return $ipnrs{$ipnr};
  }
  my $query = $dns_resolver->query($ipnr, 'PTR');


  my $fqn;
  if ($query) {
    foreach my $answer ($query->answer) {
      $fqn =  $answer -> rdatastr;
    }
  }
  else {
    $fqn =  $dns_resolver->errorstring;
  }

  $ipnrs{$ipnr} = $fqn;
  return $fqn;
}
