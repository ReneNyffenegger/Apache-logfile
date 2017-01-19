#!/usr/bin/perl
use warnings;
use strict;

use DBI;

my $db = 'ApacheLogfile.db';

die "$db already exists" if -e $db;

my $dbh = DBI->connect("dbi:SQLite:dbname=$db") or die "Could not create database $db";

$dbh -> do('create table ip (
  ipnr,
  fqn
)');

$dbh -> do('create table log (
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

