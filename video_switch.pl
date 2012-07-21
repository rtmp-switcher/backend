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
use File::Spec;
use AnyEvent;
use Linux::Inotify2;
use JSON;

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

  return $global_cfg{"ipc_dir"} . "/video_sw-" . $id . ".in";
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

# Controlling ffmpeg output
sub parseFfmpegLog($) {
  my $str = shift;
  chomp($str);

  # Do nothing for now
  _log "ffmpeg: " . $str;
};

# Controlling ffmpeg output
sub parseRTMPLog($) {
  my $str = shift;
  chomp($str);

  # Do nothing for now
  _log "rtmpdump: " . $str;
};


# Launched processes
# Key: 'out' or 'in'
# Value: AnyEvent added to read the process stderr/stdout
my %launched_proc;

# Launches handler for the outgoing channel
# Returns the pid of the launched process
# Input argument 1: Incoming channel ID
sub launchOutChanHandler($ $) {
  my $id_in = shift;
  my $id_out = shift;

  my $cmd = "ffmpeg -re -i " . getFIFOname($id_in) . getChanCmd($id_out);
  my $pid = open(IH, "$cmd 2>&1 |");
  $launched_proc{"OUT"} = AnyEvent->io (
      fh   => \*IH,
      poll => "r",
      cb   => sub { my $tst =  <IH>; parseFfmpegLog($tst); }
  );

  _log "Launched: " . $cmd;

  return $pid;
};

# Launches incoming channel handler
# Returns the pid of the launched process
# Input argument: Incoming channel ID
sub launchInChanHandler($) {
  my $id = shift;

  my $cmd = getChanCmd($id) . "-o " . getFIFOname($id);

  my $pid = open(PH, "$cmd 2>&1 |");

  $launched_proc{"IN"} = AnyEvent->io (
      fh   => \*PH,
      poll => "r",
      cb   => sub { my $tst =  <PH>; parseRTMPLog($tst); }
  );

  _log "Launched: " . $cmd;

  return $pid;
}

# Handler for the "PING" task
sub handlePing($ $) {
  _log "Got the PING: '" . $_[1] . "'";
};

# Handler for the "CONNECT" task
sub handleConnect($ $) {
  my $json_txt = $_[1];
  _log "Got CONNECT: '" . $json_txt . "'";

  # @chans will store ids of the channels
  my $json = new JSON;
  my $chans_r = $json->decode($json_txt);
  my @chans = @$chans_r;
  assert(@chans eq 2);

  my %chan_tps;
  foreach(@chans) { $chan_tps{$_} = getChanType($_); };
  # One of the channels should be incoming, another - outgoing
  assert($chan_tps{$chans[0]} ne $chan_tps{$chans[1]});

  # Launching the channel handlers
  cleanProcesses();
  launchInChanHandler($chans[1]);
  launchOutChanHandler($chans[1], $chans[0]);
};

# Handles new tasks. After processing the task, removes the task file
# Input parameter: Name of the task file
sub handleTask($) {
   my $fname = shift;
   _log "Reading task from the file '$fname'";

   # Handler for the unknown task
   sub handleDefault($ $) {
     _log "The task '" . shift . "' is unknown. Not processed BUT WILL BE REMOVED!.";
   };

   # Setting the handler for the task based on the file name pattern
   my $handler = \&handleDefault;
   if($fname =~ m/^(\w+)-\d+\.task$/) {
     if($1 eq 'PING') {
       $handler = \&handlePing;
     } elsif($1 eq 'CONNECT') {
       $handler = \&handleConnect;
     };
   }

   # Reading first line of the file
   my $full_fname = File::Spec->catfile($global_cfg{"tasks_dir"}, $fname);
   open(INP, "< $full_fname") or log_die "Couldn't open $full_fname for reading: $!";
   my $cmd = <INP>;
   if(!defined($cmd)) { $cmd = ''; }; # To handle empty files
   chomp($cmd);
   $handler->($fname, $cmd);
   close INP;

   # Removing the task file from the file system
   unlink($full_fname) or log_die "Can't delete $full_fname: $!";
   _log "Removed handled task '$fname'";
};

# Initializes tasks listener
{
  my $inotify_w;

  # Initializes tasks listener
  sub initTasksListener() {
    my $tasks_dir = $global_cfg{"tasks_dir"};

    _log "Folder to be monitored for new task files: '" . $tasks_dir . "'";

    # Create the directory if it does not exist
    unless(-e $tasks_dir or mkdir $tasks_dir) {
		log_die "Unable to create $tasks_dir";
    }

    # Starting to monitor
    my $dir_inotify = Linux::Inotify2->new;

    my $dir_w = $dir_inotify->watch(
      $tasks_dir,
      IN_MOVED_TO|IN_CLOSE_WRITE,
      sub {
        my $e = shift;
        handleTask($e->name);
      }
    );

    $inotify_w = AnyEvent->io (
       fh => $dir_inotify->fileno, poll => 'r', cb => sub { $dir_inotify->poll }
    );
  };
};

# Parsing configuration file
parse_config(\%global_cfg);

# Database connection
$dbh = DBI->connect("DBI:mysql:" . $global_cfg{"data_source"},
                    $global_cfg{"db_user"},
                    $global_cfg{"db_pswd"}, { RaiseError => 0, AutoCommit => 0 })
or log_die "Database connection not made: $DBI::errstr";

_log "Connected to the database " . $global_cfg{"data_source"};

# Initializing the caches
InitDbCache($dbh);
cleanProcesses();
createFIFOs();

# enable event loop
my $cv = AnyEvent->condvar;

initTasksListener();

launchInChanHandler(1);
launchOutChanHandler(1, 5);

# Wait for events
AnyEvent->condvar->recv;

# Finalization
DoneDbCache();
cleanProcesses();

$dbh->disconnect();

