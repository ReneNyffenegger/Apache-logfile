package ApacheLogDB;

use warnings;
use strict;

use Exporter;

use Time::Piece;
use DBI;

our @ISA = 'Exporter';
our @EXPORT = qw( $dbh shorten_fqn t_2_date_string date_string_2_t);
our $dbh;
my  $t_1970;

BEGIN { # {

  my $db = "$ENV{digitales_backup}renenyffenegger.ch/logs/log.db";
  
  die unless -e $db;
  
  $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";
  $dbh->{AutoCommit}=0;

  $t_1970 = Time::Piece->strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S');

} # }

sub shorten_fqn { # {
  my $fqn  = shift;
  my $ipnr = shift;

  if ($fqn eq 'SERVFAIL' or $fqn eq 'NXDOMAIN') {
    $fqn = $ipnr
  }
  elsif ($fqn =~ /\.([^.]+)\.([^.]+)\.$/) {
    $fqn = "$1.$2";
  }

  return $fqn;
} # }

sub t_2_date_string { # {
  my $t = shift;
  my $t_ = $t_1970 + $t;
  return $t_ -> ymd() . ' ' . $t_ -> hms();
} # }

sub date_string_2_t {

  my $date_string = shift;

  my $t_ = Time::Piece->strptime($date_string, '%Y-%m-%d %H:%M:%S');

  return $t_ - $t_1970;
}
1;
