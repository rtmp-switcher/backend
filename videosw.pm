##
## Common subroutines used by RTMP video switcher backend scripts
##
## Copyright Vitaly Repin <vitaly.repin@gmail.com>.  GPL.
##

package videosw;
require Exporter;
use DBI;
use Data::Table;
use IO::File;
use File::Spec;
use Carp::Assert;
use Config;

@ISA = qw(Exporter);
@EXPORT = qw(parse_config initLogFile InitDbCache DoneDbCache RegisterSQL ModifyDbValues GetCachedDbTable GetCachedDbValue _log log_die getChanType getChanTypeId getLatestChanCmd getBkpFolder getBkpFname incrConnectAttemps my_time my_time_short);

use strict;
use vars qw(@ISA @EXPORT $VERSION);

# Initializes the log file
# If the file does not exists, creates it. Otherwise opens in "append" mode.
# If it was not possible to open the file, error message is printed to stderr and the script dies
# Input parameter: path to the log file
sub initLogFile($);

# Logs message to stderr and log file (only if it was initialized by initLogFile subroutine)
# Input parameter: message to log
sub _log($);

# Replacement to the die subroutine. Calls _log subroutine
sub log_die($);

# Parse config file
sub parse_config ($);

# Initialize database handling routines of this modules
# Input parameter: Database handle
sub InitDbCache($);

# Database handling routines finalization
sub DoneDbCache();

# Registers SQL-statement in the system
# Param 1: SQL-statement key (to be used in calls to GetCachedDbTable and GetCachedDbValue)
# Param 2: SQL statement itself (can and typically contains "?" placeholders for the binding vars
# Param 3: 1 if result of the query should be cached. 0 if the query should be executed every time
sub RegisterSQL($ $ $);

# Modify database content (INSERT, DELETE and UPDATE SQL statements are supported)
# SQL statement should be registered with RegisterSQL
# Param 1: SQL-statement key (th esame as used in RegisterSQL)
# Param 2: List of values to insert
# Param 3: Should database commit be called (1) or not (anything else).
sub ModifyDbValues($ $ $);

sub GetCachedDbValue($ $);

sub GetCachedDbTable($ $);

# Get channel type id for the specified channel id
sub getChanType($);

# Get channel type id by its name
sub getChanTypeId($);

# Returns the list: (channel_details_id, command_line_for_the_channel_depending_on_its_type)
# If no channel_details exists for the channel specified, undef is returned in place of channel_details_id
# If connect_attempts for the latest channel detail record is more than 2, undef is returned in place of cmdline.
# Input argument: channel id
sub getLatestChanCmd($);

# Get backup folder for the specified channel id
sub getBkpFolder($);

# Get the current file name for the recording
sub getBkpFname($);

# Increment connect_attempts counter by 1
# Input argument: connection_details id
sub incrConnectAttemps($);

## Returns time strings
sub my_time ();
sub my_time_short ();

################################################################################

## return time string
sub my_time ()
{
 my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =  localtime(time);
 return sprintf "%4d-%02d-%02d_%02d_%02d_%02d", $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

## return short time string
sub my_time_short ()
{
 my $a = my_time;
 $a =~ s/_\d+_\d+$//ig;
 return $a."h";
}


# Logging functions
{
  # Log file handle
  my $log_file = undef;

  sub initLogFile($) {
    my $fname = shift;

    $log_file = IO::File->new($fname, "a+") or log_die "Couldn't open log file $fname: $!\n";
    $log_file->autoflush(1);

    return 0;
  };

  sub _log($) {
    my $str = scalar localtime() . " " . shift . "\n";

    print STDERR $str;
    if(defined($log_file)) {
        $log_file->print($str);
    };
  };
};

sub log_die($) {
 my $str = shift;

 _log  "<critical-error>$str</critical-error>";

 die $str;
};

# Expands "~" under UNIX. Does nothing under MS Windows platfrom
# Receipe 7.3 from Perl CookBook: http://docstore.mik.ua/orelly/perl/cookbook/ch07_04.htm
# Input: path to expand
# Output: expanded path or original path
sub expand_home_dir ($) {
  my $path = shift;

  if ($Config{osname} !~ /mswin/i) {
    $path =~ s{ ^ ~ ( [^/]* ) }
               { $1
                     ? (getpwnam($1))[7]
                     : ( $ENV{HOME} || $ENV{LOGDIR}
                          || (getpwuid($>))[7]
                       )
    }ex;
  };

  return $path;
};

# Parse config file
sub parse_config ($) {
   my $UP = shift;

   ## default config in UNIX
   my $cfg_fname = '~/.videoswitcher/videoswitcher.conf';

   ## MSWindows vs Linux initial config default path
   if ($Config{osname} =~ m/mswin/i) {
     ## default config in Windows
     $cfg_fname = 'c:\videosw\videoswitcher.conf';
   };

   $cfg_fname = expand_home_dir($cfg_fname);

   my ($var, $value);
   my %path_keys = ("rtmpdump_log_dir" => 1, "ffmpeg_log_dir" => 1);

   open CFG, "<$cfg_fname" or log_die ("Couldn't open cfg-file '$cfg_fname': $!");
   while (<CFG>) {
    chomp;
    s/#.*//;
    s/^\s+//;
    s/\s+$//;
    next unless length;
    ($var, $value) = split(/[=|\s ]{1,}/, $_, 2);

    ## remove '' and ""
    $value=~s/^['"]//;
    $value=~s/['"];*$//;

    ## config keys to lowercase
    $var = lc $var;

    # Expanding ~ in paths
    if(exists($path_keys{$var})) {
      $value = expand_home_dir($value);
    };

    $$UP{$var} = $value;
   }

   return 1;
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

  sub InitDbCache($) {
    $db = shift;

    # Channel types dictionary
    RegisterSQL("chan_types", "SELECT id FROM channel_types WHERE chan_type = ?", 1);

    # Retrieves channel type for the specified channel
    RegisterSQL("chan_type_by_id", "SELECT chan_type FROM channels WHERE id = ?", 0);

    # Get the latest and greatest channels params from the channel_details table
    RegisterSQL("chan_details", "SELECT id, app, playPath, flashVer, swfUrl, url, pageUrl, tcUrl, connect_attempts " .
                                "FROM channel_details WHERE channel = ? ORDER BY tm_created DESC LIMIT 1", 0);

    # Get the backup folder for the specified channel id
    RegisterSQL("bkp_folder", "SELECT bkp_folder FROM channels WHERE id = ?", 0);

    # Increments connect_attempts counter for specified channel connection details id
    RegisterSQL("incr_conn_cntr", "UPDATE channel_details SET connect_attempts = connect_attempts + 1 WHERE id = ?", 0);
  };

  sub DoneDbCache() {
    foreach (keys %st) { $st{$_}->finish(); };
  };

  sub RegisterSQL($ $ $) {
    my $key = shift;

    if(exists($sql{$key})) { return undef; }

    $sql{$key} = shift;

    my $cache_enabled = shift;
    if ($cache_enabled eq 1) {
        _log "Cache is enabled for SQL $key!";
        $caches{$key} = {};
    };
    return 1;
  };

  sub ModifyDbValues($ $ $) {
    my $key = shift;
    my $id_ref = shift;
    my $commit_needed = shift;

    unless(exists($st{$key})) {
      # First usage. Preparing the statement
      $st{$key} = $db->prepare($sql{$key}) or log_die "preparing '$sql{$key}' for '$key': " . $db->errstr;
    };

    # Inserting, deleting or updating the values
    $st{$key}->execute(@$id_ref) or log_die "executing: " . $st{$key}->errstr;

    # Commit if needed
    if($commit_needed eq 1) {
       $db->commit or log_die $db->errstr;
    };
  };

  sub GetCachedDbTable($ $) {
     my $key = shift;
     my $id_ref = shift; my $id = (join ",", @$id_ref);

     my $cache_ref = undef;

     if(exists($caches{$key})) { $cache_ref = $caches{$key}; };

     if(exists($st{$key})) {
     	if((defined($cache_ref)) && (exists($$cache_ref{$id}))) {
                _log "Returning the CACHED value for the key $key";
		        return $cache_ref->{$id};
        }
     } else {
	      $st{$key} = $db->prepare($sql{$key}) or log_die "preparing '$sql{$key}' for '$key': " . $db->errstr;
     };

     # Not in the cache or should not be cached => retrieving from the database
     my $i = 1;
     foreach(@$id_ref) {
	    $st{$key}->bind_param($i++, $_) or log_die "binding: " . $st{$key}->errstr;
     };
     $st{$key}->execute() or log_die "executing: " . $st{$key}->errstr;

     my $r = new Data::Table([], $st{$key}->{NAME_uc});
     while(my @d = $st{$key}->fetchrow_array) { $r->addRow(\@d); };

     if(defined($cache_ref)) { $$cache_ref{$id} = $r; };

     _log "Returning the FETCHED value for the key $key";
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

# Increment connect_attempts counter by 1
# Input argument: connection_details id
sub incrConnectAttemps($) {
   my @args = ($_[0]);
   return ModifyDbValues("incr_conn_cntr", \@_, 1);
};

# Get channel type id by its name
sub getChanTypeId($) {
   return  GetCachedDbValue("chan_types", \@_);
};

# Get channel type id for the specified channel id
sub getChanType($) {
   return  GetCachedDbValue("chan_type_by_id", \@_);
};

# Get backup folder for the specified channel id
sub getBkpFolder($) {
   return  GetCachedDbValue("bkp_folder", \@_);
};

# Get the current file name for the recording
sub getBkpFname($) {
  my $id = shift;
  my $folder = getBkpFolder($id);

  my $res = "/dev/null";
  if($folder ne '') {
    $res = File::Spec->catfile(expand_home_dir($folder), "ch$id-" . my_time() . ".flv");
  };

  return $res;
};

sub getLatestChanCmd($) {
   my $id = shift;
   my @args = ($id);

   my $cd = GetCachedDbTable("chan_details", \@args);
   assert($cd->isEmpty ne 1);

   if($cd->nofRow ne 1) {
     _log "ERR: getLatestChanCmd: [$id] got " . $cd->nofRow . " rows instead of 1.";
     return (undef, undef);
   };

   # Creating command line based on the query results
   my $row = $cd->rowHashRef(0);
   if($row->{"CONNECT_ATTEMPTS"} > 2) {
     _log "ERR: getLatestChanCmd: [$id] connect_attempts == " . $row->{"CONNECT_ATTEMPTS"} . " for the detail " .
          $row->{"ID"};
     return ($row->{"ID"}, undef);
   };

   my $res; # Resulting string

   my $chan_type = getChanType($id);
   if($chan_type eq getChanTypeId('RTMP_IN')) {
      # Forming command for the incoming channel
      $res = "rtmpdump -v -r \"" . $row->{"URL"} . "\" -y \"" . $row->{"PLAYPATH"} .
             "\"  -W \"http://" . $row->{"SWFURL"} .  "\" -p \"http://" . $row->{"PAGEURL"} .
             "\" -a \"" . $row->{"APP"} . "\" ";
   } elsif($chan_type eq getChanTypeId('RTMP_OUT')) {
      # Forming command for the outgoing channel
      my $url = $row->{"URL"};
      if((defined($row->{"TCURL"})) && ($row->{"TCURL"})) { $url .= $row->{"TCURL"}; };
      $res = " -codec copy -f flv \"" . $url . "\"";
   } else {
      _log "Unknown channel type " . $chan_type . " for the channel " . $id;
   };

   _log "getLatestChanCmd: returning cmdline for channel details " . $row->{"ID"};
   return ($row->{"ID"}, $res);
};

1;
