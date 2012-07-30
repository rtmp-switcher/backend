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

## Include common VideoSwitch library
use videosw;

## Global VARs ##################################################################

## read configuration parameters from file
my %global_cfg=();

my ($ch_bkp_dir, $bkp_dir);

# Parsing configuration file
parse_config(\%global_cfg);

# Open Logfile
$global_cfg{mon_log_dir}= "/tmp" if ( ! defined $global_cfg{mon_log_dir} ); 
initLogFile ($global_cfg{mon_log_dir}."/video_switch_mon_".my_time_short.".log");

# define temp dir
$global_cfg{mon_tmp_dir}="/tmp" if ( ! -w $global_cfg{mon_tmp_dir} ); 

# define file size limit to 50M
$global_cfg{limit_size}=50000000 if ( $global_cfg{limit_size} !~ /^\d+$/ );

# define wait time 
$global_cfg{mon_wait_time}=2 if ( $global_cfg{mon_wait_time} !~ /^\d+$/ );

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
    
}
# parent continues here, pid of child is in $pid
return $pid;

}


  # Database connection
  my $dbh = DBI->connect("DBI:mysql:" . $global_cfg{"data_source"},
                    $global_cfg{"db_user"},
                    $global_cfg{"db_pswd"}, { RaiseError => 0, AutoCommit => 0 })
  or log_die "Database connection not made: $DBI::errstr";

  _log "Connected to the database " . $global_cfg{"data_source"};

  # Cached SQL statements
  {
    # Get list of channels by channel type
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

while (1) {

  _log " --- Start ----------------------------";

  ## get channel status from server DB
  my $ch_stat = getChansStatus(getChanTypeId("RTMP_IN"));

  ## get channels from server DB
  my $ch_tbl = getChansByType(getChanTypeId("RTMP_IN"));

  ## get PIDs rtmpdump record command running on server
  my @out = `ps axo pid,ppid,cmd | grep -i "rtmpdump" | grep -i "rtmp_record" | grep -v grep`;

  ## get channel states from server DB 
  my $ch_states = getChanStates();

  my %pid_from_OS=();
  my %pid_from_OS_with_child=();

  ## ------------------------------------------
  ## create OS rtmpdump processes hash

  foreach my $p (@out) {

    ## extract pid and ppid from ps output
    my ($pid,$ppid,$command) = $p=~/^\s*(\d+)\s+(\d+)\s+(.*)$/;
    $pid_from_OS{$pid}=$ppid if ($pid =~ /^\d+$/);
     
    ## defined if process has a parent and parent process has a child
    $pid_from_OS_with_child{$ppid} = $pid if ( defined $pid_from_OS{$ppid} );

  } ## end foreach

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
	my $id=$r->{CHANNEL};

	if (defined $id) {
	  foreach my $k (keys %$r ) {

	    ## write status of channel to hash
	    $CH{$id}{$k}= $r->{$k};
  	  }

	  ## delete status of desabled channel
	  if ($CH{$id}{IS_ENABLED} != 1) {

	    _log 'delete status of disabled channel: $id';
	    delete $CH{$id};
	    my @a=($id);
	    ModifyDbValues("delete_from_status", \@a, 1);
	  
	  
	  } ## end if
        } ## end if 

  } ## end for

  ## ------------------------------------------
  ## write status in channel hash 
  ## on OS PID information

  foreach my $ch (keys %CH) {
   if ( defined $CH{$ch}{PID}) {

     if (defined $pid_from_OS{ $CH{$ch}{PID} }) {
       _log ("Ok! Find channel:$ch record process.");

       my $pid=$CH{$ch}{PID};

       delete $pid_from_OS{ $pid };
       delete $pid_from_OS{ $pid_from_OS_with_child {$pid} };

       $CH{$ch}{CHECKSTATUS} = 'WORKED';

     } ##end if   

   } ##end if 

  } ## end foreach

  ## ------------------------------------------
  ## Get first file sizes for worked 
  ## rtmpdump processes

  foreach my $ch (keys %CH) {

    if ( $CH{$ch}{CHECKSTATUS} eq 'WORKED' && -e $CH{$ch}{RECORDED_FNAME} ) {

      $CH{$ch}{FIRST_SIZE} = -s $CH{$ch}{RECORDED_FNAME};

      ## we need start new record process when file limit exceed
      if ( $CH{$ch}{FIRST_SIZE} > $global_cfg{limit_size} ) {
        
        $CH{$ch}{CHECKSTATUS} = 'FILE_LIMIT_EXCEED';      
	_log "On channel: $ch File size limit: $global_cfg{limit_size} exceed: $CH{$ch}{RECORDED_FNAME}";
      
      }

    } ## end if

  } ## end foreach

  ## ------------------------------------------
  ## kill rtmpdump processes with bad PID 
  ## (not exist in DB)

  foreach my $pid (keys %pid_from_OS) {

    ## kill child processes and processes without childs
    if ( ( $pid_from_OS{$pid} != 1) || ( $pid_from_OS{$pid} == 1 && ! defined $pid_from_OS_with_child{$pid} ) ) { 
    
      _log ("Kill process with pid: $pid, because not find in database");
      `kill $pid`;

      ## sleep 1 second, check process and if find it in memory, then send -9
      sleep (1);
      my $str = `ps axo pid,ppid,cmd | grep -i "rtmpdump" | grep -i "rtmp_record" | grep $pid |  grep -v grep`;
      $str =~ /^\s*(\d+)\s*.*$/mg;
      `kill -9 $pid` if ($pid eq $1);

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
  ## start RTMPDUMP command 

  foreach my $ch (sort keys %CH) {

    if ( $CH{$ch}{CHECKSTATUS} =~ /(INITIAL|FILE_LIMIT_EXCEED)/ ) {

      ## get RTMP command and RTMP URL id
      my ($ch_details_id,$cmd) = getLatestChanCmd($ch);

      ## chech that command not Empty
      if ($cmd =~ /rtmpdump/) {

        ## channel_record_filename
        my $file=$CH{$ch}{CHANNEL_BKP_FOLDER}."/rtmp_record_ch".$ch."_time_".my_time.".rtmp";

        $cmd=$cmd." -o $file 2>$file.err_log 1>$file.log";

        _log "Starting on Channel: $ch command: $cmd";

        my $res;
        my $res = start_cmd($cmd);

	## check process is started
        if ( $res =~ /^\d+$/ ) {

          ## kill old process when restart rtmpdump
	  if ($CH{$ch}{CHECKSTATUS} eq 'FILE_LIMIT_EXCEED') {
	     _log "Kill old record process for channel: $ch. File size limit exceed.";
	    `kill $pid_from_OS_with_child{$CH{$ch}{PID}}`;
	    `kill $CH{$ch}{PID}`;
	    sleep (1);
	    `kill -9 $pid_from_OS_with_child{$CH{$ch}{PID}}`;
	    `kill -9 $CH{$ch}{PID}`;

	    $CH{$ch}{CHECKSTATUS} = 'WORKED_AFTER_RESTART';
	    _log "Start new file record for channel: $ch";

	  } else {
	  
	    $CH{$ch}{CHECKSTATUS} = 'STARTED';
	  
	  }## end if

          $CH{$ch}{PID} = $res;
	  $CH{$ch}{CHECKED_DETAILS} = $ch_details_id;
          $CH{$ch}{RECORDED_FNAME}=$file;
	  _log "Succesful started command for channel: $ch";


        } else {
      
            _log "Can't start command for channel: $ch, see logs for details";
	   $CH{$ch}{CHECKSTATUS} = 'FAILED_START';
	    
        } ## end if 
      
      } else {
      
	   _log "Cant start empty command for channel: $ch";
	   $CH{$ch}{CHECKSTATUS} = 'EMPTY_RTMP_COMMAND';

      }## end if

     } ## end if

  } ## end foreach

  ## ------------------------------------------
  ## sleep after start RTMP commands

  sleep ($global_cfg{mon_wait_time});

  ## ------------------------------------------
  ## update status in hash after start commands 

  foreach my $ch (keys %CH) {
    
     if ($CH{$ch}{CHECKSTATUS} =~ /(STARTED|WORKED_AFTER_RESTART)/ ) {
       ## check PID in memory
       my $str = `ps axo pid,ppid,cmd | grep -i "rtmpdump" | grep -i "rtmp_record" | grep "$CH{$ch}{PID}" |  grep -v grep`;
       $str =~ /^\s*(\d+)\s*.*$/mg;
       $CH{$ch}{CHECKSTATUS} = 'FAILED_START' if ( $1 !~ /^\d+$/);
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

    if ( $CH{$ch}{CHECKSTATUS} eq 'WORKED' && defined $CH{$ch}{FIRST_SIZE} ) {

     $CH{$ch}{SECOND_SIZE} = -s $CH{$ch}{RECORDED_FNAME};

      if ( abs($CH{$ch}{SECOND_SIZE} - $CH{$ch}{FIRST_SIZE}) < 1 ) {

          ## restart if file size does not grow
          $CH{$ch}{CHECKSTATUS} = 'NEED_RESTART';

	  _log "Restart! File size:$CH{$ch}{FIRST_SIZE} does not grow: $CH{$ch}{RECORDED_FNAME}";   

      }

    } ## end if

  } ## end foreach


  ## ------------------------------------------
  ## start writing channel status to DB

  foreach my $ch (keys %CH) {

     $CH{$ch}{MESSAGE}=$CH{$ch}{CHECKSTATUS};

     if ($CH{$ch}{CHECKSTATUS} eq 'FAILED_START') {
     
        incrConnectAttemps( $CH{$ch}{CHECKED_DETAILS} ); 
     
     } ## end if

     if ( $CH{$ch}{CHECKSTATUS} =~/^(FAILED_START|EMPTY_RTMP_COMMAND|NEED_RESTART)$/ ) {
       $CH{$ch}{STATE}=$chan_states{DOWN};
       $CH{$ch}{PID} = "";
       $CH{$ch}{RECORDED_FNAME}=""
     
     } ## end if


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

#  print Dumper %CH;

  _log " --- Stop -----------------------------";

} ## end while

## Disconnect from DB
DoneDbCache();
$dbh->disconnect;


