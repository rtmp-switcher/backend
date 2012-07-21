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
use POSIX qw(mkfifo strftime :errno_h);
use IO::File;
use Fcntl qw(F_GETFL F_SETFL O_NONBLOCK);
use File::Spec;
use AnyEvent;
use Linux::Inotify2;
use JSON;

# Global configuration structure
my %global_cfg;

# Database handle
my $dbh;

# Hash which stores the data about connections
# Key: id of the outgoing channel
# Value: Connection data (hash of hashes)
my %connections;

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

# Drops the conection between incoming and outgoing channels
# 1. Kills the processes associated with the connection (rtmpdump and ffmpeg)
# 2. Cleans the FIFO
# 3. Removes the connection record from %connections
sub dropConnection($) {
  my $out_id = shift;

  if(!defined($connections{$out_id})) {
    _log "No previous connection with outgoing channel $out_id exists. Nothing to clean!";
    return;
  };

  # Step 1: kill rtmpdump and ffmpeg
  foreach ("in", "out") {
    my $pid = $connections{$out_id}{$_}{"pid"};

    _log "Killing " . uc($_) . " process " . $pid . ": '" . $connections{$out_id}{$_}{"cmd"} . "'";
    kill 9, $pid; # Yes, we do it on purpose. 9 because we like it to be here!
  };

  # Step 2: re-create FIFO
  reCreateFIFO(getFIFOname($out_id));

  # Step 3: Removes the connection details from %connections
  delete($connections{$out_id});
};

# Killing all the ffmpeg and rtmpdumps
sub cleanProcesses() {
  system("killall -9 ffmpeg rtmpdump 2> /dev/null");
};

# Creates named pipes. One pipe for every incoming channel
sub createFIFOs() {
  my $ch_tbl = getChansByType(getChanTypeId("RTMP_OUT"));

  for (my $i = 0; $i < $ch_tbl->nofRow; $i++) {
    my $r = $ch_tbl->rowHashRef($i);
    my $fname = getFIFOname($r->{"ID"});

    # First - remove existing FIFOs. Just to clean up everything.
    reCreateFIFO($fname);

    _log "FIFO for outgoing channel '" . $r->{"NAME"} . "' has been created: '" . $fname . "'";
 };
};

# Writes sub-process log
# Input argument 1: file descriptor to write the log to
# Input argument 2: line to log
sub writeProcLog($ $) {
  my ($fh, $str) = @_;

  chomp($str);

  if($str ne '') {
#   _log $str;
    $str = scalar localtime() . " $str\n";
    $fh->print($str);
  };
};

# Launches the sub-process
# Input argument: hash with the 2 keys defined:
# cmd: command line to launch
# fname: the filename prefix to write stderr/stdout of the launched process
# After successfull execution the sub-routine sets:
#  - 'fname' to the full path to the log file of the launched process
#  - 'pid' to the pid of the launched process
#  - 'event' to the AnyEvent::io object created to monitor the stdout/stderr of the launched process
sub launchProcess($) {
  my $proc = shift;

  my $cmd = $$proc{"cmd"};
  my $fname = $$proc{"fname"} . "-" . my_time_short .  ".log";

  # Opening log file
  my $fh = IO::File->new($fname, "a+") or log_die "Couldn't open '$fname': $!";
  $fh->print($cmd . "\n");
  $fh->autoflush(1);
  $$proc{"fname"} = $fname;

  _log "Launching '$cmd'";
  _log "Log file: '$fname'";

  my $log_fd = IO::File->new();
  $$proc{"log_fd"} = $log_fd;
  $$proc{"pid"} = $log_fd->open("$cmd 2>&1 |") or log_die "Couldn't launch sub-process: $!";

  # Setting the output as non blocked file handle
  my $fl = fcntl($log_fd, F_GETFL, 0) or log_die "Couldn't get flags for log_fd: $!";

  fcntl($log_fd, F_SETFL, $fl | O_NONBLOCK) or log_die "Couldn't set flags for log_fd: $!";

  $$proc{"event"} = AnyEvent->io (
                       fh   => $log_fd,
                       poll => "r",
                       cb   => sub {
                                 my $buf;
                                 my $rv = sysread($log_fd, $buf, 1024);
                                 unless(!defined($rv) && $! == EAGAIN) {
                                     writeProcLog($fh, $buf);
                                 };
                               }
  );

  return 1;
};

# Launches incoming channel handler
# Returns the pid of the launched process
# Input argument 1: Incoming channel ID
# Input argument 2: Outgoing channel ID
# Output argument 3: Reference to hash array to store the process data (pid, cmd etc)
sub launchInChanHandler($ $ $) {
  my ($id_in, $id_out, $proc_stat) = @_;

  my $cmd = getChanCmd($id_in) . "-o " . getFIFOname($id_out);
  my $fname_prefix = File::Spec->catfile($global_cfg{"rtmpdump_log_dir"}, "rtmpdump");

  $$proc_stat{"cmd"} = $cmd; $$proc_stat{"fname"} = $fname_prefix;
  launchProcess($proc_stat);
}

# Launches handler for the outgoing channel
# Input argument 1: Outgoing channel ID
# Output argument 2: Reference to hash array to store the process data (pid, cmd etc)
sub launchOutChanHandler($ $) {
  my ($id_out, $proc_stat) = @_;

  my $flv_fname = getBkpFname($id_out);

  my $cmd = "cat " . getFIFOname($id_out) . " | tee $flv_fname | ffmpeg -i - " . getChanCmd($id_out);

  my $fname_prefix = File::Spec->catfile($global_cfg{"ffmpeg_log_dir"}, "ffmpeg");

  $$proc_stat{"cmd"} = $cmd; $$proc_stat{"fname"} = $fname_prefix;
  launchProcess($proc_stat);
};

# Handler for the "PING" task
sub handlePing($ $) {
  my $out_id;
  _log "Got the PING: '" . $_[1] . "'";

  _log "Number of active connections: " . scalar keys %connections;

  foreach $out_id (keys %connections) {
    _log "Connection to outgoing channel $out_id:";
    my $dir;
    foreach $dir ("in", "out") {
      foreach (keys %{$connections{$out_id}{$dir}}) {
         _log "  " . uc($dir) . " chan data '$_' = '" . $connections{$out_id}{$dir}{$_} . "'";
      };
    };
  };
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

  my $id_in = $chans[0];
  my $id_out = $chans[1];

  if(getChanType($id_in) ne getChanTypeId("RTMP_IN")) {
    _log "Channel $id_in is not incoming channel. Ignoring the task.";
    return;
  };
  if(getChanType($id_out) ne getChanTypeId("RTMP_OUT")) {
    _log "Channel $id_out is not outgoing channel. Ignoring the task.";
    return;
  };

  # Launching the channel handlers
  _log "Connecting channels $id_in and $id_out";
  dropConnection($id_out);

  my %proc_stat_in; my %proc_stat_out;
  launchInChanHandler($id_in, $id_out, \%proc_stat_in);
  launchOutChanHandler($id_out, \%proc_stat_out);

  my %con; # Connection data. Hash of hashes
  $con{"in"} = \%proc_stat_in;
  $con{"out"} = \%proc_stat_out;
  $con{"in"}{"id"} = $id_in;
  $con{"out"}{"id"} = $id_out;

  $connections{$id_out} = \%con;

  # Update database tables
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
  my $dir_inotify;
  my $dir_w;

  # Initializes tasks listener
  sub initTasksListener() {
    my $tasks_dir = $global_cfg{"tasks_dir"};

    _log "Folder to be monitored for new task files: '" . $tasks_dir . "'";

    # Create the directory if it does not exist
    unless(-e $tasks_dir or mkdir $tasks_dir) {
		log_die "Unable to create $tasks_dir";
    }

    # Starting to monitor
    $dir_inotify = Linux::Inotify2->new;

    $dir_w = $dir_inotify->watch(
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

# Wait for events
AnyEvent->condvar->recv;

# Finalization
DoneDbCache();
cleanProcesses();

$dbh->disconnect();

