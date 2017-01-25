package ApacheLogDB;

use warnings;
use strict;

use Exporter;
use DBI;

our @ISA = 'Exporter';
our @EXPORT = qw( $dbh );
our $dbh;

BEGIN {

  my $db = "$ENV{digitales_backup}renenyffenegger.ch/logs/log.db";
  
  die unless -e $db;
  
  $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "$db does not exist";
  $dbh->{AutoCommit}=0;


}

1;
