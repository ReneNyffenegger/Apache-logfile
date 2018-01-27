#!/usr/bin/perl
use warnings;
use strict;

open (my $in, '<', 'access_log.processed') or die;

my %path_counter;
while (my $ln = <$in>) {

  if ($ln =~ /GET (\S+)/) {

    my $path = $1;

    next if $path eq '/';
    next if $path =~m !^/favicon.ico!;
    next if $path =~m !^/apple-touch!;
    next if $path =~m !^/rss.xml!;
    next if $path =~m !^/robots.txt!;

    $path_counter{$path}++;

  }

}

for my $path ( sort {$path_counter{$a} <=> $path_counter{$b}} keys %path_counter) {

  printf "%5d %s\n", $path_counter{$path}, $path;

}
