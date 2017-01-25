package ApacheLogDB;

use warnings;
use strict;

use Exporter;
use DBI;

our @ISA = 'Exporter';
our @EXPORT = qw( $dbh shorten_fqn);
our $dbh;

BEGIN {

  my $db = "$ENV{digitales_backup}renenyffenegger.ch/logs/log.db";
  
  die unless -e $db;
  
  $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";
  $dbh->{AutoCommit}=0;


}

sub shorten_fqn {
  my $fqn  = shift;
  my $ipnr = shift;

  if ($fqn eq 'SERVFAIL' or $fqn eq 'NXDOMAIN') {
    $fqn = $ipnr
  }
  elsif ($fqn =~ /\.([^.]+)\.([^.]+)\.$/) {
    $fqn = "$1.$2";
  }

  return $fqn;
}

1;
