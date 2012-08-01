#!/usr/bin/perl

##
## RTMP live monitoring and writer
##
## Copyright Oleg Gavrikov <oleg.gavrikov@gmail.com>.  GPL.
##

##Include standart Perl library
use strict;
use IO::File;
use LWP;
use Data::Dumper;
use JSON;
use DBI;
use POSIX qw(setsid);
use LWP::Simple;


## Include common VideoSwitch library
use videosw;

## Global VARs ##################################################################

## vide_switcher database ID
my $dbid="";

## read configuration parameters from file
my %global_cfg=();

my ($ch_bkp_dir, $bkp_dir);

# Parsing configuration file
parse_config(\%global_cfg);

# Open Logfile
$global_cfg{mon_log_dir} = "/tmp" if ( ! defined $global_cfg{mon_log_dir} ); 

# define temp dir
$global_cfg{mon_tmp_dir} = "/tmp" if ( ! -w $global_cfg{mon_tmp_dir} ); 

# define file size limit to 50M
$global_cfg{limit_size} = 50000000 if ( $global_cfg{limit_size} !~ /^\d+$/ );

# define wait time 
$global_cfg{mon_wait_time} = 2 if ( $global_cfg{mon_wait_time} !~ /^\d+$/ );

## Subs #########################################################################
# Get list of channels by channel type
sub getChansByType($) {
   my @args = shift;
   return  GetCachedDbTable("chans_by_type", \@args);
};

sub getConnStatus() {
   return  GetCachedDbTable("connect_status",\@_);
};

sub getChanStates() {
   return  GetCachedDbTable("chan_states",\@_);
};


#sub getChanRecordPid ($) {
#   my @args = ($_[0]);
#   return  GetCachedDbValue("chans_pid", \@args);
#
#};

sub databasename () {
 my $a = GetCachedDbTable("database_name", \@_);

 my $b=$a->rowHashRef(0);
 return ($b->{'DATABASE()'});

}

sub getChansStatus {
   my @args = shift;
   return  GetCachedDbTable("chans_status", \@args);
};

sub start_cmd ($) {
 my $cmd=shift;
 my $pid = fork();

 die "unable to fork: $!" unless defined($pid);
 if (!$pid) {  # child
     exec($cmd);
     log_die "unable to exec: $!";
 } ## end  if

# parent continues here, pid of child is in $pid
return $pid;

} ## end sub;

## kill rtmpdump with childs
sub kill_rtmp_by_pid ($$) {

 my ( $pid, $pid_from_OS_with_child ) = @_;
 my (%pids,%pids_w_ch)=();

 if ($pid=~/^\d+$/ && $pid>1 ) {

       `kill $$pid_from_OS_with_child{$pid}` if (defined $$pid_from_OS_with_child{$pid});
       `kill $pid`;
       sleep (1);

       `kill -9 $$pid_from_OS_with_child{$pid}` if (defined $$pid_from_OS_with_child{$pid}) ;
       `kill -9 $pid`;
 } ## end if

} ## end sub


## get PIDs rtmpdump record command running on server
sub get_rtmp_processes ($$) {

  my ($pid_from_OS, $pid_from_OS_with_child) = @_;

  my $rtmp_processes_command_mask='ps axo pid,ppid,cmd | grep -i "rtmpdump" | grep -i "_rec_ch" | grep "$dbid" | grep -v grep';
  my @out = `$rtmp_processes_command_mask`;

  foreach my $p (@out) {

    ## extract pid and ppid from ps output
    my ($pid,$ppid,$command) = $p=~/^\s*(\d+)\s+(\d+)\s+(.*)$/;
    $$pid_from_OS{$pid}=$ppid if ($pid =~ /^\d+$/);

    ## defined if process has a parent and parent process has a child
    $$pid_from_OS_with_child{$ppid} = $pid if ( defined $$pid_from_OS{$ppid} );

  } ## end foreach
} ## end sub


# here is where we make ourself a daemon
sub daemonize {
 chdir '/' or log_die "Can't chdir to /: $!";
 open STDIN, "/dev/null" or log_die "Can't read /dev/null: $!";
 open STDOUT, ">>/dev/null" or log_die "Can't write to /dev/null: $!";
 open STDERR, ">>/dev/null" or log_die "Can't write to /dev/null: $!";
 defined(my $pid = fork) or log_die "Can’t fork: $!";
 exit if $pid;
 setsid or log_die "Can't start a new session: $!";
 umask 0;
} ## end sub

# flush the buffer
  $| = 1;

# daemonize the program
#  &daemonize;

 
  # Database connection
  my $dbh = DBI->connect("DBI:mysql:" . $global_cfg{"data_source"},
                    $global_cfg{"db_user"},
                    $global_cfg{"db_pswd"}, { RaiseError => 0, AutoCommit => 0 })
 or die "Database connection not made: $DBI::errstr";
 

  # Cached SQL statements
  {
    # Get list of channels by channel type
    RegisterSQL("database_name", "SELECT database()", 0);

    RegisterSQL("chans_by_type", "SELECT * FROM channels WHERE chan_type = ?", 0);

    RegisterSQL("chan_states", "SELECT * FROM channel_states", 0);

    RegisterSQL("connect_status", "SELECT * FROM connections order by tm_created", 0);

    RegisterSQL("chans_status","select c.*, cs.* from channels c, channel_status cs where c.chan_type = ? and c.id = cs.channel",0);

    RegisterSQL("update_chan_status", "update channel_status set checked_details=?, state=?, pid=?, recorded_fname=?, message=? where channel=?", 0);

    RegisterSQL("insert_chan_status", "insert into channel_status (checked_details, state, pid, recorded_fname, message, channel) values (?,?,?,?,?,?)", 0);

    RegisterSQL("delete_from_status", "delete from channel_status where channel = ?", 0);
  }

  # Initializing the caches
  InitDbCache($dbh);

 $dbid=databasename;


 while (1) {
  
  initLogFile ($global_cfg{mon_log_dir}."/video_switch_mon_".my_time_short.".log");

  _log " --- Start ----------------------------";

  ## get channel status from server DB
  my $ch_stat = getChansStatus(getChanTypeId("RTMP_IN"));

  ## get channels from server DB
  my $ch_tbl = getChansByType(getChanTypeId("RTMP_IN"));

  ## get channel states from server DB 
  my $ch_states = getChanStates();

  my %pid_from_OS=();
  my %pid_from_OS_with_child=();

  ## ------------------------------------------
  ## create OS rtmpdump processes hash
  get_rtmp_processes (\%pid_from_OS,\%pid_from_OS_with_child);


  ## ------------------------------------------
  ## channel states from DB

  my %chan_states=();
  for (my $i = 0; $i < $ch_states->nofRow; $i++) {
    my $r = $ch_states->rowHashRef($i);
    $chan_states{ $r->{NAME} }= $r->{ID};

  }## end for

  ## ------------------------------------------
  ## init channel status hash

  my %CH=();

  ## init channel ID and bkp folders from database
  for (my $i = 0; $i < $ch_tbl->nofRow; $i++) {
    my $r = $ch_tbl->rowHashRef($i);

    my $id=$r->{ID};
    $CH{$id}=();
    $CH{$id}{BKP_FOLDER}=$r->{BKP_FOLDER};
    $CH{$id}{IS_ENABLED}=$r->{IS_ENABLED};
    $CH{$id}{CHECKSTATUS}='INITIAL';

  } ## end for

  ## ------------------------------------------
  ## get channel status information  
  ## from database 

  for (my $i = 0; $i < $ch_stat->nofRow; $i++) {
 	my $r = $ch_stat->rowHashRef($i);
	my $ch=$r->{CHANNEL};

	if (defined $ch) {
	  foreach my $k (keys %$r ) {

	    ## write status of channel to hash
	    $CH{$ch}{$k}= $r->{$k};
  	  }

        _log ("In DB for channel: $ch find PID: $CH{$ch}{PID}") if ($CH{$ch}{PID} =~ /^\d+$/ && $CH{$ch}{PID} > 0);

        } ## end if 

  } ## end for

  ## ------------------------------------------
  ## Delete disabled channel from channel hash 

  foreach my $ch (keys %CH) {

  ## delete status of desabled channel
    if ( $CH{$ch}{IS_ENABLED} < 1 ) {

      if ( defined $CH{$ch}{CHECKED_DETAILS} ) {

           _log "delete status of disabled channel from DB: $ch";
           my @a=($ch);
           ModifyDbValues("delete_from_status", \@a, 1);

       } ## end if

       _log "delete disabled channel from hash: $ch";
       delete $CH{$ch};

    } ## end if

  } ##end foreach
  
  
  ## ------------------------------------------
  ## write status in channel hash 
  ## on OS PID information

  foreach my $ch (keys %CH) {

   if ( $CH{$ch}{PID} =~ /^\d+$/ && $CH{$ch}{PID} > 0) {

     if (defined $pid_from_OS{ $CH{$ch}{PID} }) {
       _log ("Ok! Find working channel:$ch record process.");

       my $pid=$CH{$ch}{PID};

       delete $pid_from_OS{ $pid };
       delete $pid_from_OS{ $pid_from_OS_with_child {$pid} };

       $CH{$ch}{CHECKSTATUS} = 'WORKING';

     } ##end if   

   } ##end if 

  } ## end foreach

  ## ------------------------------------------
  ## kill rtmpdump processes with bad PID 
  ## (not exist in DB)

  foreach my $pid (keys %pid_from_OS) {

    ## kill child processes and processes without childs
    if ( ( $pid_from_OS{$pid} != 1) || ( $pid_from_OS{$pid} == 1 && ! defined $pid_from_OS_with_child{$pid} ) ) { 

      _log ("Kill process with pid: $pid, because not find in database");
      kill_rtmp_by_pid ($pid, \%pid_from_OS_with_child);

    } ## end if

  } ## end foreach


  ## ------------------------------------------
  ## check and create directory for 
  ## write channels

  foreach my $ch (keys %CH) {
    
    my $bkp_dir;

    ## set to DB value of backup dir
    if (defined $CH{$ch}{BKP_FOLDER}) {
         $bkp_dir = $CH{$ch}{BKP_FOLDER};
    } else {
         ## set to default TEMP value from config
         $bkp_dir = $global_cfg{mon_tmp_dir};
    
    } ## end if

    ## Can write to backup folder?
    if ( ! -w  $bkp_dir ) {
         _log "Can't write channel:$ch to directory: $bkp_dir";

	 ## set write directory to temp
	 $bkp_dir = $global_cfg{mon_tmp_dir};

	 _log "Set  channel:$ch write directory to: $bkp_dir";

    } 

    ## yes, we can write to backup folder
    ## create channel DIR, if not exist
    my $ch_bkp_dir = $bkp_dir."/channel_".$ch;

    if (! -e $ch_bkp_dir) {
        _log ("Create directory $ch_bkp_dir");
        mkdir $ch_bkp_dir;

    } else {

        ## Can write to channel folder?
        if ( ! -w $ch_bkp_dir ) {

            _log "Can't write to channel dir: $ch_bkp_dir";
	    _log "Write to temp";

	    ## write to temp
	    $ch_bkp_dir = $global_cfg{mon_tmp_dir}."/channel_".$ch;
	    mkdir $ch_bkp_dir;

        } ## end if 

    } ## end if

    ## set baclup folder and channel backup folder
    $CH{$ch}{BKP_FOLDER}=$bkp_dir;
    $CH{$ch}{CHANNEL_BKP_FOLDER}=$ch_bkp_dir;

  } ## end forech   

  ## ------------------------------------------
  ## Get first file sizes for worked 
  ## rtmpdump processes

  foreach my $ch (keys %CH) {

    if ( $CH{$ch}{CHECKSTATUS} =~/^(WORKING)$/ && -e $CH{$ch}{RECORDED_FNAME} ) {

      $CH{$ch}{FIRST_SIZE} = -s $CH{$ch}{RECORDED_FNAME};

      ## we need start new record process when file limit exceed
      if ( $CH{$ch}{FIRST_SIZE} > $global_cfg{limit_size} ) {

        $CH{$ch}{CHECKSTATUS} = 'FILE_LIMIT_EXCEED';
        _log "On channel: $ch File size limit: $global_cfg{limit_size} exceed: $CH{$ch}{RECORDED_FNAME}";

      }

    } ## end if

  } ## end foreach

  ## ------------------------------------------
  ## start RTMPDUMP command 

  foreach my $ch (sort keys %CH) {

    if ( $CH{$ch}{CHECKSTATUS} =~ /(INITIAL|FILE_LIMIT_EXCEED)/ ) {

      ## get RTMP command and RTMP URL id
      my ($ch_details_id,$cmd) = getLatestChanCmd($ch);

      ## chech that command not Empty
      if ($cmd =~ /rtmpdump/) {

        ## channel_record_filename
        my $file=$CH{$ch}{CHANNEL_BKP_FOLDER}."/".$dbid."_rec_ch".$ch."_time_".my_time.".rtmp";

        $cmd=$cmd." -o $file 2>$file.err_log 1>$file.log";

        _log "Starting on Channel: $ch command: $cmd";

        my $res;
        my $res = start_cmd($cmd);

	## check process is started
        if ( $res =~ /^\d+$/ ) {

          ## kill old process when restart rtmpdump
	  if ($CH{$ch}{CHECKSTATUS} eq 'FILE_LIMIT_EXCEED') {

	     _log "Start new file record for channel: $ch";
	     $CH{$ch}{CHECKSTATUS} = 'WORKING';

	     _log "Kill old record process for channel: $ch. File size limit exceed.";
              kill_rtmp_by_pid ($CH{$ch}{PID}, \%pid_from_OS_with_child);

	  } else {
	  
	    $CH{$ch}{CHECKSTATUS} = 'STARTED';
	  
	  }## end if
          
          ## set channel hash PID, channel_details ID and record file name
          $CH{$ch}{PID} = $res;
	  $CH{$ch}{CHECKED_DETAILS} = $ch_details_id;
          $CH{$ch}{RECORDED_FNAME}  = $file;

	  ## check file size
	  $CH{$ch}{FIRST_SIZE} = -s $CH{$ch}{RECORDED_FNAME};

	  _log "Succesful started command whith PID:$res for channel: $ch";


        } else {
      
            _log "Can't start command for channel: $ch, see logs for details";
	   $CH{$ch}{CHECKSTATUS} = 'NEED_RESTART';
	    
        } ## end if 
      
      } else {
      
	   _log "Cant start empty command for channel: $ch";
	   $CH{$ch}{CHECKSTATUS} = 'EMPTY_RTMP_COMMAND';

      }## end if

     } ## end if

  } ## end foreach

  ## ------------------------------------------
  ## sleep after start RTMP commands

  _log ("Waiting: $global_cfg{mon_wait_time} sec");
  sleep ($global_cfg{mon_wait_time});

  ## ------------------------------------------
  ## update status in hash after start commands 

  ## create OS rtmpdump processes hash
  %pid_from_OS=();
  %pid_from_OS_with_child=();
  get_rtmp_processes (\%pid_from_OS,\%pid_from_OS_with_child);

  foreach my $ch (keys %CH) {
    
     if ($CH{$ch}{PID}=~/^\d+$/) {

       if ( ! defined $pid_from_OS { $CH{$ch}{PID} } ) {

           _log ("Can't find in memory: $CH{$ch}{PID} after start record process for channel: $ch");
           $CH{$ch}{CHECKSTATUS} = "NEED_RESTART" if ( $1 !~ /^\d+$/);

       } # end if

     } ## end if

  } ## end foreach

  ## ------------------------------------------
  ## Create Connection Task File for new 
  ## channel or restart channel

  my $con_st = getConnStatus();
  my %connect_status=();
  for (my $i = 0; $i < $con_st->nofRow; $i++) {
    my $r = $con_st->rowHashRef($i);

    if ( $CH{$r->{IN_CHAN}}{CHECKSTATUS} eq 'STARTED' ) {
      my $str=my_time;
      $str=~s/[_-]//g;
      open (FH,">$global_cfg{tasks_dir}/CONNECT-".$str.".task") || log_die "Can't write to task dir";
      print FH "[\"".$r->{IN_CHAN}."\",\"".$r->{OUT_CHAN}."\"]\n";
    
    } 

  }## end for

  ## ------------------------------------------
  ## Get second file sizes for worked
  ## rtmpdump processes

  foreach my $ch (keys %CH) {

    if ( defined $CH{$ch}{FIRST_SIZE} ) {

     $CH{$ch}{SECOND_SIZE} = -s $CH{$ch}{RECORDED_FNAME};

      if ( abs($CH{$ch}{SECOND_SIZE} - $CH{$ch}{FIRST_SIZE}) < 1 ) {
         if ( $CH{$ch}{CHECKSTATUS} =~ /^(WORKING)$/ ) {
	    _log "Channel $ch. Need restart working process. File size:$CH{$ch}{FIRST_SIZE} does not grow: $CH{$ch}{RECORDED_FNAME}";   
         } else { 
            _log "Channel $ch. Failed to start. File size:$CH{$ch}{FIRST_SIZE} does not grow: $CH{$ch}{RECORDED_FNAME}"; 
         } ## end if
      
         $CH{$ch}{CHECKSTATUS} = 'NEED_RESTART';

      } ## end if  
      
    } else {
          ## Record file size of started process is 0 
          if ( $CH{$ch}{CHECKSTATUS} =~ /^(STARTED)$/ && ( -s $CH{$ch}{RECORDED_FNAME} ) < 1 ) {

            _log "Channel $ch. Failed to start. File: $CH{$ch}{RECORDED_FNAME} size is 0";

	    $CH{$ch}{CHECKSTATUS} = 'NEED_RESTART';

          } ## end if
    } ## end if

  } ## end foreach


  ## ------------------------------------------
  ## start writing channel status to DB

  ## create OS rtmpdump processes hash
  %pid_from_OS=();
  %pid_from_OS_with_child=();
  get_rtmp_processes (\%pid_from_OS,\%pid_from_OS_with_child);

  foreach my $ch (keys %CH) {

     $CH{$ch}{MESSAGE}=$CH{$ch}{CHECKSTATUS};

     if ($CH{$ch}{CHECKSTATUS} eq 'NEED_RESTART') {
     
       ## increment connection attepms
       incrConnectAttemps( $CH{$ch}{CHECKED_DETAILS} ); 
     
       ## kill bad record processes with PID
       if ( $CH{$ch}{PID} > 0 ) {

          get_rtmp_processes (\%pid_from_OS,\%pid_from_OS_with_child);
          _log "Kill bad record process with PID: $CH{$ch}{PID} for channel: $ch. Need restart.";
          kill_rtmp_by_pid ($CH{$ch}{PID}, \%pid_from_OS_with_child);

       } ## end if

       ## set channel attributes for DB
       _log "Set to '' hash PID for channel $ch, because file size does not grow";
       $CH{$ch}{STATE}=$chan_states{DOWN};
       $CH{$ch}{PID} = "";
       $CH{$ch}{RECORDED_FNAME}=""

     
     } else {
     
       $CH{$ch}{STATE}=$chan_states{UP};
       resetConnectAttemps ( $CH{$ch}{CHECKED_DETAILS} );
     
     }## end if

     ## insert new status in channel status
     if (! defined $CH{$ch}{URI} && defined $CH{$ch}{CHECKED_DETAILS}) {

       # checked_details, state, pid, recorded_fnamae, message, channel
       my @a=($CH{$ch}{CHECKED_DETAILS}, $CH{$ch}{STATE}, $CH{$ch}{PID}, $CH{$ch}{RECORDED_FNAME}, $CH{$ch}{MESSAGE}, $ch);

       ModifyDbValues("insert_chan_status", \@a, 1);

       next;

     } ## end if

     ## update channel status
     if ( defined $CH{$ch}{URI} ) {

       # checked_details, state, pid, recorded_fnamae, message, channel
       my @a=($CH{$ch}{CHECKED_DETAILS}, $CH{$ch}{STATE}, $CH{$ch}{PID}, $CH{$ch}{RECORDED_FNAME},  $CH{$ch}{MESSAGE}, $ch);
     
       ModifyDbValues("update_chan_status", \@a, 1);

       next;
     
     } ## end if

  } ## end foreach

  print Dumper %CH;

  _log " --- Stop -----------------------------";

} ## end while

## Disconnect from DB
DoneDbCache();
$dbh->disconnect;

