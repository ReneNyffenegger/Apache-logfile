#!/usr/bin/perl
use warnings;
use strict;

my $archive_dir    = "$ENV{digitales_backup}renenyffenegger.ch/logs/archive/";


  transform('2014-10-15_13-41-31___2014-10-16_03-25-17', 'access_log.2014_10a');
  transform('2014-10-16_08-40-38___2014-10-31_03-49-17', 'access_log.2014_10b');
# transform('2014-10-31_05-39-19___2014-11-30_02-59-59', 'access_log.2014_11');
# transform('2014-11-30_05-45-45___2014-12-31_02-18-31', 'access_log.2014_12');
# transform('2014-12-31_06-16-12___2015-01-31_05-07-05', 'access_log.2015_01');
# transform('2015-01-31_17-27-47___2015-02-25_22-49-41', 'access_log.2015_02');
# transform('2015-02-26_06-48-39___2015-03-31_03-36-00', 'access_log.2015_03');
# transform('2015-03-31_06-07-43___2015-04-30_02-18-09', 'access_log.2015_04');
# transform('2015-04-30_06-35-10___2015-05-31_04-25-53', 'access_log.2015_05');
# transform('2015-05-31_05-29-48___2015-06-30_01-59-01', 'access_log.2015_06');
# transform('2015-06-30_06-44-44___2015-07-31_04-00-28', 'access_log.2015_07');
# transform('2015-07-31_06-57-46___2015-08-31_04-34-08', 'access_log.2015_08');
# transform('2015-08-31_21-39-16___2015-09-30_04-23-24', 'access_log.2015_09');
# transform('2015-09-30_04-56-56___2015-10-31_04-13-14', 'access_log.2015_10');
# transform('2015-10-31_05-05-32___2015-11-29_23-32-53', 'access_log.2015_11');
# transform('2015-11-30_06-09-59___2015-12-31_04-47-32', 'access_log.2015_12');
# transform('2015-12-31_10-09-27___2016-01-31_02-20-51', 'access_log.2016_01');
# transform('2016-01-31_07-55-41___2016-02-29_03-14-36', 'access_log.2016_02');
# transform('2016-02-29_05-07-01___2016-03-31_01-58-33', 'access_log.2016_03');
# transform('2016-03-31_09-49-39___2016-04-30_03-39-56', 'access_log.2016_04');
# transform('2016-04-30_05-20-12___2016-05-31_04-39-06', 'access_log.2016_05');
# transform('2016-05-31_05-46-48___2016-06-30_05-33-22', 'access_log.2016_06');
# transform('2016-06-30_05-42-34___2016-07-31_04-18-10', 'access_log.2016_07');
# transform('2016-07-31_05-45-11___2016-08-31_04-58-16', 'access_log.2016_08');
# transform('2016-08-31_05-42-55___2016-09-29_23-28-11', 'access_log.2016_09');
# transform('2016-09-30_01-48-47___2016-10-31_00-01-54', 'access_log.2016_10');
# transform('2016-10-31_01-36-25___2016-11-29_23-09-56', 'access_log.');


sub transform {
  my $from = shift;
  my $to   = shift;

  print "$to already exists\n" if -e $to;

  open (my $src,  '<', "$archive_dir$from");
  open (my $dest, '>', "$archive_dir$to"  );

  while (my $line = <$src>) {
    $line =~ s/\[[^]]+\] //;
    print $dest $line;
  }

  close $src;
  close $dest;
  
}
