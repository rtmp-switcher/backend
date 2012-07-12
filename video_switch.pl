#!/usr/bin/perl -w
#
#       video_switch.pl. July 2012
#
#       Connects incoming video streams (RTMP) to outgoing RTMP stream.
#
#       Copyright Vitaly Repin <vitaly.repin@gmail.com>.  GPL.
#

use strict;
use videosw;
use Carp::Assert;
use POSIX qw(mkfifo);
use AnyEvent;
#use Linux::Inotify2;

# Global configuration structure
my %global_cfg;

# Database handle
my $dbh;

# Cached SQL statements
{
  # Channel states
  RegisterSQL("chan_stats", "SELECT id FROM channel_states WHERE name = ?", 1);

  # Get URI of the outgoing channel
  RegisterSQL("out_chan_uri", "SELECT uri from channels WHERE id = ?", 0);

   # Get list of channels by channel type
  RegisterSQL("chans_by_type", "SELECT id, name FROM channels WHERE is_enabled = TRUE AND chan_type = ?", 0);
}

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

# Returns the name of the FIFO used to communicate between rtmpdump and ffmpeg
# Input argument: channel id
sub getFIFOname($) {
  my $id = shift;

  return $global_cfg{ipc_dir} . "/video_sw-" . $id . ".in";
};

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

# Killing all the ffmpeg and rtmpdumps
sub cleanProcesses() {
  system("killall -9 ffmpeg rtmpdump 2> /dev/null");
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

sub parseFfmpegLog($) {
  _log "kkkk";
};

# Launches handler for the outgoing channel
# Returns the pid of the launched process
# Input argument 1: Incoming channel ID
# Input argument 2: Outgoing channel ID
my $wait_for_input;
sub launchOutChanHandler($ $) {
  my $id_in = shift;
  my $id_out = shift;

  my $cmd = "ffmpeg -i " . "/home/vit/rec.flv " . getChanCmd($id_out);
  my $pid = open(PH, "$cmd 2>&1 |");

  $wait_for_input = AnyEvent->io (
      fh   => \*PH,
      poll => "r",
      cb   => sub { my $tst =  <PH>; _log $tst; }
  );

  _log $cmd;
  return $pid;
};

# Launches incoming channel handler
# Returns the pid of the launched process
# Input argument: Incoming channel ID
sub launchInChanHandler($) {
  my $id = shift;

  my $cmd = getChanCmd($id) . "-o " . getFIFOname($id);
  _log $cmd;
}

# Parsing configuration file
parse_config(\%global_cfg);

# Database connection
$dbh = DBI->connect("DBI:mysql:" . $global_cfg{data_source},
                    $global_cfg{db_user},
                    $global_cfg{db_pswd}, { RaiseError => 0, AutoCommit => 0 })
or log_die "Database connection not made: $DBI::errstr";

_log "Connected to the database " . $global_cfg{data_source};

# Initializing the caches
InitDbCache($dbh);
cleanProcesses();
createFIFOs();

# enable event loop
my $cv = AnyEvent->condvar;

launchInChanHandler(1);
launchOutChanHandler(2, 5);

# Wait for events
#AnyEvent->condvar->recv;

# Finalization
DoneDbCache();
cleanProcesses();

$dbh->disconnect();

