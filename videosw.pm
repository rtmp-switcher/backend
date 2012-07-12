##
## Common subroutines used by RTMP video switcher backend scripts 
## 
## Copyright Vitaly Repin <vitaly.repin@gmail.com>.  GPL.
## 

package videosw;
require Exporter;   
use DBI;
use Data::Table;

@ISA = qw(Exporter); 
@EXPORT = qw(parse_config get_cfg_param InitDbCache DoneDbCache RegisterSQL GetCachedDbTable GetCachedDbValue _log log_die);

use strict;
use vars qw(@ISA @EXPORT $VERSION);

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

1;
