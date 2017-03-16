#!/usr/bin/perl
use warnings;
use strict;
use Time::HiRes qw(time);
# use Time::Piece;
use HTTP::BrowserDetect;
use Net::DNS::Resolver;
use Geo::IP;
use Getopt::Long;
use Time::Piece;

use ApacheLogDB;

GetOptions('last-month'  => \my $get_last_month) or die;

my @files = qw(access_log access_log.processed);
if ($get_last_month) {
    my $date_time = new Time::Piece;
    my $month = $date_time->add_months(- 1); # Subtract one month
    my $archive_file_name = $month->strftime('access_log.%Y_%m');
    push @files, $archive_file_name;
}

my $geo_ip_file = '/usr/local/share/GeoIP/GeoIPCity.dat';
my $geo_ip_file_age_s = (stat($geo_ip_file))[9];
printf "$geo_ip_file is %.1f days old\n", (time - $geo_ip_file_age_s) / 24 / 3600;

my $parse_robot_from_agent_string = 1;

my $dns_resolver = new Net::DNS::Resolver;
my %ipnrs;
my $ipnrs_found_in_cache = 0;
my $ipnrs_looked_up = 0;

my $geoip   = Geo::IP->open($geo_ip_file, GEOIP_STANDARD) or die;

my $t_max_in_log = ($dbh->selectrow_array('select max(t) from log'))[0];
printf "Max t in log: $t_max_in_log (%s)\n", t_2_date_string($t_max_in_log);

my $rec_cnt = 0;

my   $insert_sth = $dbh->prepare("insert into log (t, method, path, status, referrer, requisite, rogue, robot, gip_country, gip_city, ipnr, fqn, agent, size  )values (?, ?, ? , ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") or die;

my $already_inserted_sth = $dbh->prepare('select count(*) cnt from log indexed by log_t_ix where t = :1 and ipnr = :2 and path = :3 and method = :4') or die;

my $start_t = time;
my $total_t_inserted_sth = 0;
my $total_t_insert_sth   = 0;
my $total_t_ipnr2fqn     = 0;


while (my $log_f = shift @files) {
  load_log_file("$ENV{digitales_backup}renenyffenegger.ch/logs/archive/$log_f");
}

my $end_t = time;

$dbh -> do("insert into load_hist values($start_t, $end_t, $t_max_in_log)");
$dbh -> commit;

printf("loaded %i records in %5.2f seconds (%7.2f recs/s)\n", $rec_cnt, $end_t - $start_t, $rec_cnt/($end_t - $start_t));
printf("   %7.4f seconds for sth inserted\n", $total_t_inserted_sth);
printf("   %7.4f seconds for sth insert\n"  , $total_t_insert_sth);
printf("   %7.4f seconds for ipnr2fqn\n"    , $total_t_ipnr2fqn);
print ("   ipnrs in cache: $ipnrs_found_in_cache, ipnrs lookeed up: $ipnrs_looked_up\n");


sub load_log_file { #_{
  my $log_f = shift;
  print "Loading $log_f\n";

  open (my $log_h, '<', $log_f) or die "could not open $log_f";

  while (my $log_l = <$log_h>) {

    my $do_insert = 1;

    my $rogue = 0;
    my $robot = '';
  
    if ( my ($year, $month, $day, $hour, $min, $sec, $ipnr, $method, $path, $http, $status, $size, $referrer, $agent) = $log_l =~ m!^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d) (\d+\.\d+\.\d+\.\d+) "(\w+) (.*) (HTTP/1\.\d)" (\d+) (\d+) "([^"]*)" "([^"]*)"!) {

      my $method_; # {
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
      elsif ($method eq 'OPTIONS') {
        $method_ = 'O';
      }
      else {
        die "method = $method"
      } # }


#     my $t_line = Time::Piece->strptime("$year-$month-$day $hour:$min:$sec", '%Y-%m-%d %H:%M:%S');
#     my $t =$t_line - $t_1970; 
      my $t = date_string_2_t("$year-$month-$day $hour:$min:$sec");

      my $sth_inserted_start_t = time;
      $already_inserted_sth -> execute($t, $ipnr, $path, $method_);
      my $sth_inserted_end_t = time;
      $total_t_inserted_sth += ($sth_inserted_end_t - $sth_inserted_start_t);

      my $cnt = ($already_inserted_sth -> fetchrow_array)[0];

      if ($t > $t_max_in_log) {

        # if $t > $t_max_in_log then we should find no
        # record in the db.
        # If we find one, something has gone terribly wrong, let's
        # die then:

        my $t_diff = $t - $t_max_in_log;

        if ($cnt > 0 ) { # {

#         printf("
#           
#           t[$t (%s)] > t_max_in_log[$t_max_in_log],
#           Diff: $t_diff
#           File: $log_f
#           Line: $.
#           IP:   $ipnr
#           Path: $path
#           cnt:  $cnt\n\n", t_2_date_string($t));

        } # }

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

        my $ipnr2fqn_start_t = time;
        my $fqn = ipnr_2_fqn($ipnr);
        my $ipnr2fqn_end_t   = time;
        $total_t_ipnr2fqn += ($ipnr2fqn_end_t - $ipnr2fqn_start_t);

        if ($parse_robot_from_agent_string) {
          if ($fqn eq 'spider.tiger.ch.') {
            $robot = 'spider.tiger.ch';
          }
          elsif ($referrer eq 'http://semalt.semalt.com/crawler.php?u=http://renenyffenegger.ch') {
            $robot = 'semalt.com';
          }
          else {
            my $ua = new HTTP::BrowserDetect($agent);
            $robot = $ua->robot_string || '';
          }
        }
  
        $rogue = is_rogue($path, $referrer, $ipnr);


        my $requisite = 0;
        if ($path eq '/favicon.ico' or $path eq '/robots.txt' or $path =~ /\.(png|css|png|jpg|woff|js|gif|php|pdf|ttf)$/ or $path eq '/font/cartogothicstd-book-webfont.eot?') {
          $requisite = 1;
        }

        my $gip_rec     = $geoip->record_by_addr($ipnr);
        my $gip_country = $gip_rec->{country_code};
        my $gip_city    = $gip_rec->{city};
      
        my $sth_insert_start_t = time;
        $insert_sth -> execute($t, $method_, $path, int($status), $referrer, $requisite, $rogue, $robot, $gip_country, $gip_city, $ipnr, $fqn, $agent, $size);
        my $sth_insert_end_t = time;
        $total_t_insert_sth += ($sth_insert_end_t - $sth_insert_start_t);

        $rec_cnt++;
      }
    }
    else {
      die "Could not parse\n$log_l\n";
    }
  
  }

} #_}
sub is_rogue { #_{
  my $path     = shift;
  my $referrer = shift;
  my $ipnr     = shift;

  return 1 if $path =~ m!/[Ff]?ckeditor!; #_{
  return 1 if $path =~ m!/typo3/!;
  return 1 if $path =~ m!^/wordpress/!;
  return 1 if $path =~ m!^/scripts/!;
  return 1 if $path =~ m!^/plugins/!;
  return 1 if $path =~ m!^/assets/!;
  return 1 if $path =~ m!^/assetmanager/!;
  return 1 if $path =~ m!^/joomla!;
  return 1 if $path =~ m!/editor.*/editor/!;
  return 1 if $path =~ m!^/editor/!;
  return 1 if $path =~ m!^/index\.php!;
  return 1 if $path =~ m!/wp-!;
  return 1 if $path =~ m!^/admin/.*\.sql$!;
  return 1 if $path =~ m!^/license\.!;
  return 1 if $path =~ m!^/cleaner[^.]*\.sh$!;
  return 1 if $path =~ m!^/admin!;
  return 1 if $path =~ m!^/xmlrpc.php!;
  return 1 if $path =~ m!^/kcfinder/!;
  return 1 if $path =~ m!^/tiki!;
  return 1 if $path =~ m!^/www\.sql\.!;
  return 1 if $path =~ m!^/web\.sql\.!;
  return 1 if $path =~ m!^/users\.!;
  return 1 if $path =~ m!^/upload\.!;
  return 1 if $path =~ m!^/temp\.!;
  return 1 if $path =~ m!^/sql\.!;
  return 1 if $path =~ m!^/site\.!;
  return 1 if $path =~ m!^/home\.!;
  return 1 if $path =~ m!^/mysql\.!;
  return 1 if $path =~ m!^/dbase\.!;
  return 1 if $path =~ m!^/dump\.!;
  return 1 if $path =~ m!^/data\.!;
  return 1 if $path =~ m!^/dbdump\.!;
  return 1 if $path =~ m!^/dbadmin\.!;
  return 1 if $path =~ m!^/backup\.!;
  return 1 if $path =~ m!^/db\.!;
  return 1 if $path eq  '/wp/';
  return 1 if uc($path) eq  '/README.txt';
  return 1 if length($path) > 500;
 #_}
  $referrer = '' unless $referrer;

  return 1 if $referrer eq   'http://rebelmouse.com/'                                                                        and $path eq '/'; #_{
  return 1 if $referrer eq   'http://buttons-for-website.com'                                                                and $path eq '/';
  return 1 if $referrer eq   'http://www.webwiki.ch/www.renenyffenegger.ch'                                                  and $path eq '/';
  return 1 if $referrer eq   'http://1-99seo.com/try.php?u=http://renenyffenegger.ch'                                        and $path eq '/';
  return 1 if $referrer eq   'http://buttons-for-your-website.com'                                                           and $path eq '/';
  return 1 if $referrer eq   'http://burger-imperia.com/'                                                                    and $path eq '/';
  return 1 if $referrer =~m !^http://success-seo-com/!                                                                       and $path eq '/';
  return 1 if $referrer eq   'http://1-free-share-buttons.com'                                                               and $path eq '/';
  return 1 if $referrer eq   'http://blog.societyforexcellenceineducation.org/cat-31/india-dissertation-help-writing.html'   and $path eq '/';
  return 1 if $referrer =~m !^http://www.combib.de/!;
  return 1 if $referrer =~m !^http://www.biblestudytools.com!;
  return 1 if $referrer =~m !^http://www.firmenpresse.de/!;
  return 1 if $referrer =~m !^http://www.viandpet.com/!;    
  return 1 if $referrer =~m !^http://www.bible.com/!;       
  return 1 if $referrer =~m !^http://www.obohu.cz/!;       
  return 1 if $referrer eq   'http://buylyricamrx.com';
  return 1 if $referrer eq   'http://pizza-tycoon.com/';
  return 1 if $referrer eq   'http://uptime.com/renenyffenegger.ch';
 #_}

  return 1 if $path eq '/notes/development/languages/Perl/modules/WWW/Mechanize/Firefox/index/'  and $referrer =~ m|^https?://[^/]+/$|;

  return 1 if $ipnr    eq '185.81.157.145'; # 2017-02-28
  return 1 if $ipnr    eq '218.71.150.87' ; # 2017-03-16
  return 1 if $ipnr    eq '61.174.160.35' ; # 2017-03-16

  return 0;
} #_}

sub ipnr_2_fqn { #_{
  my $ipnr = shift;

  if (exists $ipnrs{$ipnr}) {
    $ipnrs_found_in_cache++;
    return $ipnrs{$ipnr};
  }
  $ipnrs_looked_up++;
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
} #_}
