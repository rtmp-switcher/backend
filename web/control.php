<?php

require dirname(__FILE__) . '/utils/KLogger.php';

// phpinfo();

$hostname = "localhost";
$dbname   = "video_switch";
$username = "video_switch";
$password = "RhjkkbR5%";


date_default_timezone_set("Europe/Vatican"); # For KLogger not to show warnings
$log = new KLogger('/tmp/', KLogger::DEBUG); # Specify the log directory
$log->logInfo("control.php started, IP: " . $_SERVER['REMOTE_ADDR']);

try {
    $dbh = new PDO("mysql:host=$hostname;dbname=$dbname", $username, $password);

    // getChannels: returns the channels data
    if(isset($_GET['getChannels'])) {
	$sql = "SELECT channels.id, channels.name, channels.uri, channel_types.chan_type FROM channels, channel_types " .
	       "WHERE channels.chan_type = channel_types.id AND channels.is_enabled = TRUE";

    $stmt = $dbh->query($sql);

	// We need to return the latest valid URL for every channel. Stored in channel_details
	$sql = "SELECT url FROM channel_details WHERE tm_created = (SELECT MAX(tm_created) FROM channel_details " .
	       "WHERE channel = ?) AND channel = ?";
    $details = $dbh->prepare($sql);

	$chans = array();
	while($obj = $stmt->fetchObject()) {
        array_push($chans, $obj);
	    $details->execute(array($obj->{'id'}, $obj->{'id'}));
	    $obj->{'lastUrl'} = "";
	    while($r = $details->fetch()) { $obj->{'lastUrl'} = $r[0]; };
	}
	echo json_encode($chans);
	$log->logInfo("getChannels request is handled");
    }

    if(isset($_POST['dataType'])) {
        switch($_POST['dataType']) {
	    case 'channelDetails':
	    			$chan_data = json_decode($_POST['data']);
				$sql = "INSERT INTO channel_details (channel, app, playPath, flashVer, swfUrl, url, pageUrl, tcUrl) " .
				       "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        			$q = $dbh->prepare($sql);
				$r = $q->execute(array($chan_data->{'channel'},
						  $chan_data->{'app'},
						  $chan_data->{'playpath'},
						  $chan_data->{'flashver'},
						  $chan_data->{'swfurl'},
						  $chan_data->{'url'},
						  $chan_data->{'pageurl'},
						  $chan_data->{'tcurl'}));

				if(!$r) {
				    $log->logInfo("Error inserting channel details '" . serialize($_POST['data']) . "':" .
						      serialize($q->errorInfo()));
				} else {
                    $log->logInfo("channelDetails #" . $chan_data->{"channel"} . " are inserted into the database");
                };
				break;
	    default:
	    			$log->logInfo("Unknown dataType: " . $_POST['dataType']);
	};
   }
   // Close the database connection
   $dbh = null;
} catch(PDOException $e) {
	echo $e->getMessage();
}

?>
