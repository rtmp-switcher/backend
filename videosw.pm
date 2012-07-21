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
use Carp::Assert;
use Config;

@ISA = qw(Exporter);
@EXPORT = qw(parse_config initLogFile InitDbCache DoneDbCache RegisterSQL GetCachedDbTable GetCachedDbValue _log log_die getChanTypeId getChanCmd my_time my_time_short);

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
# Param 1: SQL-statement alias (to be used in calls to GetCachedDbTable and GetCachedDbValue)
# Param 2: SQL statement itself (can and typically contains "?" placeholders for the binding vars
# Param 3: 1 if result of the query should be cached. 0 if the query should be executed every time
sub RegisterSQL($ $ $);

sub GetCachedDbValue($ $);

sub GetCachedDbTable($ $);

# Get channel type id by its name
sub getChanTypeId($);

# Returns command line for the channel depending on its type
# Input argument: channel id
sub getChanCmd($);

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

# Parse config file
sub parse_config ($) {
   my $UP = shift;
   my $cfg_fname;

   ## MSWindows vs Linux initial config default path
   if ( $Config{osname} !~ /mswin/i ) {
    ## default config in Linux
    $cfg_fname = '~/.videoswitcher/videoswitcher.conf';
    # Receipe 7.3 from Perl CookBook: http://docstore.mik.ua/orelly/perl/cookbook/ch07_04.htm
    $cfg_fname =~ s{ ^ ~ ( [^/]* ) }
               { $1
                     ? (getpwnam($1))[7]
                     : ( $ENV{HOME} || $ENV{LOGDIR}
                          || (getpwuid($>))[7]
                       )
               }ex;

   } else {
     ## default config in Windows
     $cfg_fname = 'c:\videosw\videoswitcher.conf';

   } ## end if

   my ($var, $value);

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
    RegisterSQL("chan_type_by_id", "SELECT chan_type FROM channels WHERE id = ?", 1);

    # Get the latest and greatest channels params from the channel_details table
    RegisterSQL("chan_details", "SELECT app, playPath, flashVer, swfUrl, url, pageUrl, tcUrl FROM channel_details ".
                              "WHERE tm_created = (SELECT MAX(tm_created) FROM channel_details WHERE channel = ?) " .
                              "AND channel = ?", 0);
  };

  sub DoneDbCache() {
    foreach (keys %st) { $st{$_}->finish(); };
  };

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

# Get channel type id by its name
sub getChanTypeId($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_types", \@args);
};

# Get channel type id for the specified channel id
sub getChanType($) {
   my @args = ($_[0]);
   return  GetCachedDbValue("chan_type_by_id", \@args);
};

# Returns command line for the channel depending on its type
# Input argument: channel id
sub getChanCmd($) {
   my $id = shift;
   my @args = ($id, $id);

   my $cd = GetCachedDbTable("chan_details", \@args);
   assert($cd->isEmpty ne 1);
   assert($cd->nofRow eq 1);

   # Creating command line based on the query results
   my $row = $cd->rowHashRef(0);
   my $res; # Resulting string

   my $chan_type = getChanType($id);
   if($chan_type eq getChanTypeId('RTMP_IN')) {
      # Forming command for the incoming channel
      $res = "rtmpdump -V -v -r \"" . $row->{"URL"} . "\" -y \"" . $row->{"PLAYPATH"} .
             "\"  -W \"http://" . $row->{"SWFURL"} .  "\" -p \"http://" . $row->{"PAGEURL"} .
             "\" -a \"" . $row->{"APP"} . "\" ";
   } elsif($chan_type eq getChanTypeId('RTMP_OUT')) {
      # Forming command for the outgoing channel
      my $url = $row->{"URL"};
      if((defined($row->{"TCURL"})) && ($row->{"TCURL"})) { $url .= $row->{"TCURL"}; };
      $res = " -loglevel verbose -codec copy -f flv \"" . $url . "\"";
   } else {
      _log "Unknown channel type " . $chan_type . " for the channel " . $id;
   };

   return $res;
};

1;
