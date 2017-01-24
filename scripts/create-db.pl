#!/usr/bin/perl
use warnings;
use strict;

use Getopt::Long;

use DBI;

Getopt::Long::GetOptions(
    "force"               => \my $force,
) or die;

my $db = 'ApacheLogfile.db';

if (-e $db) {
  if ($force) {
    print "$db already exists, removing it\n";
    unlink $db;
  }
  else {
    die "$db already exists";
  }
}

my $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "Could not create database $db";

$dbh -> do('create table ip (
  ipnr,
  fqn
)');

$dbh -> do('create table log (
  id        integer primary key autoincrement,
  t                ,   -- seconds since 1970-01-01 (unix epoch) 
  method           ,   -- G[ET], P[OST]
  path             ,   -- /robots.txt
--HTTP/1.1         ,   --
  status    int    ,   -- 200 
  referrer         ,   --
  --               ,   --
  rogue     int(1) ,   -- 0, 1
  robot            ,   -- 
  --               ,   --
  ipnr             ,   -- IP Number aaa.bb.ccc.ddd
  agent            ,   --
  size                 --
)');

