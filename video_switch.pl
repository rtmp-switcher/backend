#!/usr/bin/perl -w

use strict;
use DBI;
use Data::Table;

# Global configuration structure
my %global_cfg;

## data_source  MySql database name
$global_cfg{data_source} = "video_switch:video.perestroike2.net";
## db_user		Database user
$global_cfg{db_user} = "video_switch";
## db_pswd      Database user's password
$global_cfg{db_pswd} = "Nhe,fleh2?";

## Database queries caching
{
  # Prepared database statements
  my %st;
  # Database SQL statements
  my %sql;
  # Ссылки на caches
  my %caches;
  # Database handle
  my $db;

  sub InitDbCache($) { $db = shift; };

  sub DoneDbCache() {
    foreach (keys %st) { $st{$_}->finish(); };
  };

  sub RegisterSQL($ $) {
    my $key = shift;

    if(exists($sql{$key})) { return undef; }

    $sql{$key} = shift;
    $caches{$key} = {};
    return 1;
  };

  sub GetCachedDbTable($ $) {
     my $key = shift;
     my $id_ref = shift; my $id = (join ",", @$id_ref);
     my $f_n =  (caller(0))[3];

     my $cache_ref = $caches{$key};

     if(exists($st{$key})) {
     	if(exists($$cache_ref{$id})) {
		    return $cache_ref->{$id};
     	};
     } else {
	  $st{$key} = $db->prepare($sql{$key})
     	  or die "preparing '$sql{$key}' for '$key': " . $db->errstr;
     };

     # Not in the cache => retrieving from the database
     my $i = 1;
     foreach(@$id_ref) {
	    $st{$key}->bind_param($i++, $_) or die "binding: " . $st{$key}->errstr;
     };
     $st{$key}->execute() or die "executing: " . $st{$key}->errstr;
     my $r = new Data::Table([], $st{$key}->{NAME_uc});

     while(my @d = $st{$key}->fetchrow_array) {
	    $r->addRow(\@d);
     };

     $$cache_ref{$id} = $r;

     return $r;
  };

  sub GetCachedDbValue($ $) {
     my $t = GetCachedDbTable(shift, shift);

     if($t->nofRow eq 0) {
     	return '';
     } elsif($t->nofCol eq 1) {
	return $t->elm(0, 0);
     } else {
        my %r;
	foreach($t->header) { $r{$_} = $t->elm(0, $_); };
	return \%r;
     };
  };
};

# Cached SQL statements
{
  # Channel types dictionary
  RegisterSQL("chan_types", "SELECT id FROM channel_types WHERE chan_type = ?");

  # Task tables dictionary
  RegisterSQL("task_types", "SELECT id FROM task_types WHERE name= ?");

  # Channel states
  RegisterSQL("chan_stats", "SELECT id FROM channel_states WHERE name = ?");
}

# Get channel type id by its name
sub getChanTypeId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_types", \@args);
};

# Get task type id by its name
sub getTaskTypeId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("task_types", \@args);
};

# Get channel state id by name
sub getChanStateId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_stats", \@args);
};


# Database connection
my $dbh = DBI->connect("DBI:mysql:" . $global_cfg{data_source},
                    $global_cfg{db_user},
                    $global_cfg{db_pswd}, { RaiseError => 0, AutoCommit => 0 })
or die "Database connection not made: $DBI::errstr";

# Initializing the caches
InitDbCache($dbh);

# Example:
print "RTMP_IN: " . getChanTypeId("RTMP_IN") . "\n";
print "RTMP_OUT: " . getChanTypeId("RTMP_OUT") . "\n";

print "Task type SYNC: " . getTaskTypeId("SYNC") . "\n";

print "Channel type DOWN: " . getChanStateId("DOWN") . "\n";

# Finalization
DoneDbCache();
$dbh->disconnect();



