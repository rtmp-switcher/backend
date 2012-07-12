#!/usr/bin/perl -w
#
#       video_switch.pl. July 2012
#
#       Connects incoming video streams (RTMP) to outgoing RTMP stream.
#
#       Copyright Vitaly Repin <vitaly.repin@gmail.com>.  GPL.
#

use strict;
use Carp::Assert;
use DBI;
use Data::Table;
use POSIX qw(mkfifo);
#use Linux::Inotify2;

# Global configuration structure
my %global_cfg;

# Database handle
my $dbh;

sub parse_config ($) {
   my $UP = shift;
   my $cfg_fname = '/home/vit/.videoswitcher/videoswitcher.conf';

   my ($var, $value);

   open CFG, "<$cfg_fname" or log_die ("Couldn't open cfg-file '$cfg_fname': $!");
   while (<CFG>) {
    chomp;
    s/#.*//;
    s/^\s+//;
    s/\s+$//;
    next unless length;
    ($var, $value) = split(/[=|\s ]{1,}/, $_, 2);
    $$UP{$var} = $value;
   }
 return 1;
};

sub get_cfg_param ($ $) {
  my ($str, $UP)=@_;

  if(!exists $$UP{$str}) {
  		log_die ("Can't find parameter '$str' in the configuration file");
  };

  return $$UP{$str};
}

# Read the configuration file
# Configuration parameters are written into global_cfg hash
sub get_config () {

   my %cfg=();

   parse_config(\%cfg);
   $global_cfg{data_source} = get_cfg_param("data_source", \%cfg);
   $global_cfg{db_user} = get_cfg_param("db_user", \%cfg);
   $global_cfg{db_pswd} = get_cfg_param("db_pswd", \%cfg);
   $global_cfg{ipc_dir} = get_cfg_param("ipc_dir", \%cfg);
};


## Database queries caching
{
  # Prepared database statements
  my %st;
  # Database SQL statements
  my %sql;
  # References to the caches
  my %caches;
  # Database handle
  my $db;

  sub InitDbCache($) { $db = shift; };

  sub DoneDbCache() {
    foreach (keys %st) { $st{$_}->finish(); };
  };

  # Registers SQL-statement in the system
  # Param 1: SQL-statement alias (to be used in calls to GetCachedDbTable and GetCachedDbValue)
  # Param 2: SQL statement itself (can and typically contains "?" placeholders for the binding vars
  # Param 3: 1 if result of the query should be cached. 0 if the query should be executed every time
  sub RegisterSQL($ $ $) {
    my $key = shift;

    if(exists($sql{$key})) { return undef; }

    $sql{$key} = shift;

    my $cache_enabled = shift;
    if ($cache_enabled) {
        $caches{$key} = {};
    };
    return 1;
  };

  sub GetCachedDbTable($ $) {
     my $key = shift;
     my $id_ref = shift; my $id = (join ",", @$id_ref);
     my $f_n =  (caller(0))[3];

     my $cache_ref = undef;

     if(exists($caches{$key})) { $cache_ref = $caches{$key}; };

     if(exists($st{$key})) {
     	if((defined($cache_ref)) && (exists($$cache_ref{$id}))) {
		        return $cache_ref->{$id};
        }
     } else {
	      $st{$key} = $db->prepare($sql{$key})
     	  or die "preparing '$sql{$key}' for '$key': " . $db->errstr;
     };

     # Not in the cache or should not be cached => retrieving from the database
     my $i = 1;
     foreach(@$id_ref) {
	    $st{$key}->bind_param($i++, $_) or die "binding: " . $st{$key}->errstr;
     };
     $st{$key}->execute() or die "executing: " . $st{$key}->errstr;
     my $r = new Data::Table([], $st{$key}->{NAME_uc});

     while(my @d = $st{$key}->fetchrow_array) { $r->addRow(\@d); };

     if(defined($cache_ref)) { $$cache_ref{$id} = $r; };

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
  RegisterSQL("chan_types", "SELECT id FROM channel_types WHERE chan_type = ?", 1);

  # Channel states
  RegisterSQL("chan_stats", "SELECT id FROM channel_states WHERE name = ?", 1);

  # Get URI of the outgoing channel
  RegisterSQL("out_chan_uri", "SELECT uri from channels WHERE id = ?", 0);

  # Get incoming channels params
  RegisterSQL("chan_params", "SELECT app, playPath, flashVer, swfUrl, url, pageUrl, tcUrl FROM channel_details ".
                             "WHERE tm_created = (SELECT MAX(tm_created) FROM channel_details WHERE channel = ?) " .
                             "AND channel = ?", 0);

  # Get list of channels by channel type
  RegisterSQL("chans_by_type", "SELECT id, name FROM channels WHERE is_enabled = TRUE AND chan_type = ?", 0);
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

# Get list of channels by channel type
sub getChansByType($) {
   my @args = ($_[0]);
   return  GetCachedDbTable("chans_by_type", \@args);
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

# Returns the latest channel_details row for specified channel id
# Input argument: channel id
sub getChanCmd($) {
   my @args = ($_[0], $_[0]);

   my $t = GetCachedDbTable("chan_params", \@args);
   assert($t->isEmpty ne 1);
   assert($t->nofRow eq 1);

   return $t;
};

# Returns the ffmpeg parameters to use to broadcast the outgoing stream
# The only parameter which is not formed by this subroutine is "-i <source>"
# Input argument: outgoing channel id
sub getOutCmd($) {
   my $t = getChanCmd(shift);

   # Outgoing RTMP URL is stored in the "URL" column.
   # Suffix is stored in the "TCURL" column
   # See architecture specs for the details: https://github.com/rtmp-switcher/backend/wiki/Architecture
   my $r = $t->rowHashRef(0);
   my $url = $r->{"URL"};
   if((defined($r->{"TCURL"})) && ($r->{"TCURL"})) { $url .= $r->{"TCURL"}; };
   return " -loglevel verbose -codec copy -f flv \"" . $url . "\"";
};

# Returns the rtmpdump command line to catch the RTMP stream coming from the incoming channel
# Input argument: incoming channel id
sub getInCmd($) {
   my $chan_id = shift;
   my $t = getChanCmd($chan_id);

   # Creating rtmpdump command line based on the query results
   my $r = $t->rowHashRef(0);

   while( my ($k, $v) = each %$r ) {
        _log "key: $k, value: $v";
   }

   return "rtmpdump -v -r " . $r->{"URL"} . " -y ? -W " . $r->{"SWFURL"} . " -p " . $r->{"PAGEURL"} . " -a ? -o " . getFIFOname($chan_id);
   # rtmpdump -v -r rtmp://flash69.ustream.tv/ustreamVideo/10107870 -y "streams/live" -W "http://static-cdn1.ustream.tv/swf/live/viewer:64.swf?vrsl=c:236&ulbr=100" -p "http://www.ustream.tv/channel/titanium-sportstiming" -a "ustreamVideo/10107870" -o -
}

# Removes FIFO if it exists. Creates named pipe after.
# Input argument: FIFO path
sub reCreateFIFO($) {
  my $fname = shift;

  if(-e $fname) {
     _log "FIFO '$fname' exists. Removing";
     unlink($fname) or log_die "Can't remove '$fname': $!";
  }

  # Making named pipe
  mkfifo($fname, 0600) or log_die "mkfifo($fname) failed: $!";
  _log "FIFO has been (re-)created: '" . $fname . "'";
};

# Creates named pipes. One pipe for every incoming channel
sub createFIFOs() {
  my $ch_tbl = getChansByType(getChanTypeId("RTMP_IN"));

  for (my $i = 0; $i < $ch_tbl->nofRow; $i++) {
    my $r = $ch_tbl->rowHashRef($i);
    my $fname = getFIFOname($r->{"ID"});

    # First - remove existing FIFOs. Just to clean up everything.
    reCreateFIFO($fname);

    _log "FIFO for channel '" . $r->{"NAME"} . "' has been created: '" . $fname . "'";
 };
};

# Launches handler for the outgoing channel
# Returns the pid of the launched process
# Input argument 1: Incoming channel ID
# Input argument 2: Outgoing channel ID
sub launchOutChanHandler($ $) {
  my $id_in = shift;
  my $id_out = shift;

  my $cmd = "ffmpeg -i " . getFIFOname($id_in) . getOutCmd($id_out);
  my $pid = open(PH, "$cmd 2>&1 |");
  _log $cmd;
  return $pid;
};

# Launches incoming channel handler
# Returns the pis of the launched process
# Input argument: Incoming channel ID
sub launchInChanHandler($) {
  my $id = shift;

  my $cmd = getInCmd($id);
  _log $cmd;
}

# Parsing configuration file
get_config();

# Database connection
$dbh = DBI->connect("DBI:mysql:" . $global_cfg{data_source},
                    $global_cfg{db_user},
                    $global_cfg{db_pswd}, { RaiseError => 0, AutoCommit => 0 })
or log_die "Database connection not made: $DBI::errstr";

_log "Connected to the database " . $global_cfg{data_source};

# Initializing the caches
InitDbCache($dbh);

createFIFOs();

# Example:
print "RTMP_IN: " . getChanTypeId("RTMP_IN") . "\n";
print "RTMP_OUT: " . getChanTypeId("RTMP_OUT") . "\n";

print "Channel type DOWN: " . getChanStateId("DOWN") . "\n";

_log getInCmd(1);

launchInChanHandler(1);
launchOutChanHandler(1, 5);
# Finalization
DoneDbCache();
$dbh->disconnect();



