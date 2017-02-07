#!/usr/bin/perl
use warnings;
use strict;
use Archive::Extract;
use Time::Piece;
use Getopt::Long;
use File::Copy;
use lib "$ENV{github_top_root}lib/tq84-PerlModules";
use tq84_ftp;

GetOptions('last-month'  => \my $get_last_month) or die;

print "getting last month too\n" if $get_last_month;

my $verbose        = 1;
my $archive_dir    = "$ENV{digitales_backup}renenyffenegger.ch/logs/archive/";

my %mon2num = qw(Jan 01 Feb 02 Mar 03 Apr 04 May 05 Jun 06 Jul 07 Aug 08 Sep 09 Oct 10 Nov 11 Dec 12); # used for parse_date

die "$archive_dir does not exist" unless -d $archive_dir;

my $ftp = new tq84_ftp('TQ84_RN') or die;

$ftp -> cwd('/logs') or die;

# print join "\n", $ftp -> dir;

  get_file('access_log'               ); # today
  get_file('access_log.processed'     ); # current month until »end of« yesterday
  get_file('access_log.processed.1.gz') if $get_last_month;
# get_file('access_log.processed.2.gz') if $get_last_month;

sub get_file {
  my $remote_file_name = shift;

  $ftp -> get($remote_file_name, "downloaded/$remote_file_name") or die;

  print "get_file: $remote_file_name -> downloaded/$remote_file_name\n" if $verbose;;

  my $extracted_file_name;
  my $age_in_months;

  if ($remote_file_name =~ /(.*\.(\d+))\.gz$/) {
  
    $extracted_file_name = $1; 
    $age_in_months       = $2;
    extract_file($remote_file_name);
    unlink "downloaded/$remote_file_name" or die;;
  
  }
  else {
    $extracted_file_name = $remote_file_name;
    $age_in_months       = 0; # not needed
    move("downloaded/$remote_file_name", "extracted") or die "$! - downloaded/$remote_file_name -> extracted";
  
    print "  moved downloaded/$remote_file_name -> extracted/$remote_file_name\n";
  }

  archive_file($extracted_file_name, $age_in_months);
}

sub extract_file {
  my $file_name = shift;

  my $unzip = new Archive::Extract(archive => "downloaded/$file_name") or die;

  $unzip->extract(to=>'extracted') or die;

  print "  extract_file: $file_name extracted\n" if $verbose;
}

sub archive_file {
  my $file_name = shift;
  my $age_in_months    = shift;

  print "  archive_file: $file_name [$age_in_months]\n" if $verbose;

  my $archive_file_name;
  if ($file_name =~ /processed$/ or $file_name =~ /access_log$/) {
    $archive_file_name = $file_name;

#   print "    archive_file: renamed extracted/$file_name", "${archive_dir}$file_name\n" if $verbose;
  }
  else {
    my $date_time = new Time::Piece;
    my $month = $date_time->add_months(- $age_in_months);
    $archive_file_name = $month->strftime('access_log.%Y_%m');
  }

  if (-e "${archive_dir}$archive_file_name") {
    print "    unlink ${archive_dir}$archive_file_name\n";
    unlink "${archive_dir}$archive_file_name" or die;
  }

  translate_file("extracted/$file_name", "${archive_dir}$archive_file_name");
# move ("extracted/$file_name", "${archive_dir}$archive_file_name") or die $!;
  print "    moved  extracted/$file_name to  ${archive_dir}$archive_file_name\n" if $verbose;


}

sub translate_file {

  my $from = shift;
  my $to   = shift;

  open (my $src,  '<', $from) or die;
  open (my $dest, '>', $to  ) or die;

  while (my $line = <$src>) {

    if (my ($ip, $timestamp, $method, $path, $http, $status, $size, $referrer, $agent) = $line =~ m!^(\d+\.\d+\.\d+\.\d+) - - \[(\d+/\w\w\w/\d\d\d\d:\d\d:\d\d:\d\d \+\d\d\d\d)\] "(\w+) (.*) (HTTP/1\.\d)" (\d+) (\d+) "([^"]*)" "([^"]*)"!) { # {{{

  #   my $addr = addr_of_ip($ip);
  
      my ($day, $month, $year, $hour, $min, $sec) = parse_date($timestamp);

      print $dest qq{$year-$month-$day $hour:$min:$sec $ip "$method $path $http" $status $size "$referrer" "$agent"\n};

    } # }}}
    else {
      print "Not mached $line\n";
    }

  }

  close $src;
  close $dest;

  unlink $from or die;

  print "  translate_file: $from -> $to\n" if $verbose;

}

sub parse_date {

  my $date = shift;

  my ($day, $month_, $year, $hour, $min, $sec) = $date =~ m!(\d+)/(\w\w\w)/(\d\d\d\d):(\d\d):(\d\d):(\d\d)!;
  my $month = $mon2num{$month_};

  return ($day, $month, $year, $hour, $min, $sec);

}
