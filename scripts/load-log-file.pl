#!/usr/bin/perl
use warnings;
use strict;
use Time::HiRes qw(time);
use HTTP::BrowserDetect;

use DBI;

my $parse_robot_from_agent_string = 1;

my $db = 'ApacheLogfile.db';

die unless -e $db;
my $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";
$dbh -> {AutoCommit} = 0;

my $rec_cnt = 0;

my $insert_sth = $dbh->prepare("insert into log values (strftime('\%s', ?), ?, ?, ?, ?, ?, ?, ?, ?, ?)") or die;

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
    $rec_cnt++;


    my $rogue = 0;
    my $robot = '';
  
    if ( my ($year, $month, $day, $hour, $min, $sec, $ipnr, $addr, $method, $path, $http, $status, $size, $referrer, $agent) = $log_l =~ m!^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d) (\d+\.\d+\.\d+\.\d+) \[([^]]+)\] "(\w+) (.*) (HTTP/1\.\d)" (\d+) (\d+) "([^"]*)" "([^"]*)"!) {

      if ($parse_robot_from_agent_string) {
        my $ua = new HTTP::BrowserDetect($agent);
        $robot = $ua->robot_string || '';
      }

      $rogue = is_rogue($path);
  
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
    
      $insert_sth -> execute("$year-$month-$day $hour:$min:$sec", $method_, $path, int($status), $referrer, $rogue, $robot, $ipnr, $agent, $size);
    }
    else {
      die $log_l;
    }
  
  }

}
sub is_rogue {
  my $path = shift;
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
  return 0;
}
