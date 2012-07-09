#!/usr/bin/perl -w

use strict;
use Carp::Assert;
use DBI;
use Data::Table;
use POSIX qw(mkfifo);

# Global configuration structure
my %global_cfg;

# Database handle
my $dbh;

## MySql database name
$global_cfg{data_source} = "video_switch:video.perestroike2.net";
## Database user
$global_cfg{db_user} = "video_switch";
## Database user's password
$global_cfg{db_pswd} = "password";
## Directory for IPC resources (named pipes and message queues)
$global_cfg{ipc_dir} = "/tmp";

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

  # Channel states
  RegisterSQL("chan_stats", "SELECT id FROM channel_states WHERE name = ?");

  # Get URI of the outgoing channel
  RegisterSQL("out_chan_uri", "SELECT uri from channels WHERE id = ?");

  # Get incoming channels params
  RegisterSQL("in_chan_params", "SELECT app, playPath, flashVer, swfUrl, url, pageUrl, tcUrl FROM channel_details ".
                                "WHERE tm_created = (SELECT MAX(tm_created) FROM channel_details WHERE channel = ?) " .
                                "AND channel = ?");
}

# Get channel type id by its name
sub getChanTypeId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_types", \@args);
};

# Get channel state id by name
sub getChanStateId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_stats", \@args);
};

# Logs message to stderr
sub _log($) {
  my $str = shift;

  print STDERR "$str\n";
};

# Replacement to the die subroutine. Calls _log subroutine
sub log_die($) {
 my $str = shift;

 _log  "<critical-error: $str>";

 die $str;
};


# Attaches to the message queue used to receive commands from the Editor
# If the query does not exist, creates it
sub attachToMsgQueue() {
};

# Returns the name of the FIFO used to communicate between rtmpdump and ffmpeg
# Input argument: channel id
sub getFIFOname($) {
  my $id = shift;

  return $global_cfg{ipc_dir} . "/video_sw-" . $id . ".in";
};

# Returns the ffmpeg parameters to use to broadcast the outgoing stream
# The only parameter which is not formed by this subroutine is "-i <source>"
# Input argument: outgoing channel id
sub getOutCmd($) {
   my @args = ($_[0]);
   my $uri = GetCachedDbValue("chan_stats", \@args);
   _log "RMTP URI for the outgoing channel " . $args[0] . ": " . "'$uri'";
   return " -codec copy -f flv \"$uri\"";
};

# Returns the rtmpdump command line to catch the RTMP stream coming from the incoming channel
# Input argument: incoming channel id
sub getInCmd($) {
   my @args = ($_[0], $_[0]);

   my $t = GetCachedDbTable("in_chan_params", \@args);
   assert($t->isEmpty ne 1);
   assert($t->nofRow eq 1);

   my %r;
   foreach($t->header) { $r{$_} = $t->elm(0, $_); };

   foreach (keys %r) { _log "$_: " . $r{$_}; };

   # rtmpdump -v -r rtmp://flash69.ustream.tv/ustreamVideo/10107870 -y "streams/live" -W "http://static-cdn1.ustream.tv/swf/live/viewer:64.swf?vrsl=c:236&ulbr=100" -p "http://www.ustream.tv/channel/titanium-sportstiming" -a "ustreamVideo/10107870" -o -
}

# Creates named pipes. One pipe for every incoming channel
sub createFIFO() {
  my $sql = "SELECT id, name FROM channels WHERE is_enabled = TRUE AND chan_type = " . getChanTypeId("RTMP_IN") . ";";

  my $st = $dbh->prepare($sql);
  $st->execute() or log_die "executing: " . $st->errstr;

  my ($id, $name);
  $st->bind_columns(\$id, \$name);

  while ($st->fetch()) {
    my $fname = getFIFOname($id);

    # First - remove existing FIFOs. Just to clean up everything.
    if (-e $fname) {
       _log "FIFO '$fname' exists. Removing";
       unlink($fname) or log_die "Can't remove '$fname': $!";
    }

    # Making named pipes
    mkfifo($fname, 0600) or log_die "mkfifo($fname) failed: $!";
    _log "FIFO for channel '" . $name . "' has been created: '" . $fname . "'";
 };
};

# Database connection
$dbh = DBI->connect("DBI:mysql:" . $global_cfg{data_source},
                    $global_cfg{db_user},
                    $global_cfg{db_pswd}, { RaiseError => 0, AutoCommit => 0 })
or log_die "Database connection not made: $DBI::errstr";

_log "Connected to the database " . $global_cfg{data_source};

# Initializing the caches
InitDbCache($dbh);

createFIFO();

# Example:
print "RTMP_IN: " . getChanTypeId("RTMP_IN") . "\n";
print "RTMP_OUT: " . getChanTypeId("RTMP_OUT") . "\n";

print "Channel type DOWN: " . getChanStateId("DOWN") . "\n";

getInCmd(1);

# Finalization
DoneDbCache();
$dbh->disconnect();



