#!/usr/bin/perl
use warnings;
use strict;

use File::Copy;
use POSIX;

my $db = "$ENV{digitales_backup}renenyffenegger.ch/logs/log.db";
die unless -f $db;

my $yyyy_mm_dd = strftime('%Y-%m-%d', localtime);
my $db_yyyy_mm_dd = "$ENV{digitales_backup}renenyffenegger.ch/logs/log-$yyyy_mm_dd.db";

die if -f $db_yyyy_mm_dd;
print "Copying $db to $db_yyyy_mm_dd\n";
copy ($db, $db_yyyy_mm_dd);
