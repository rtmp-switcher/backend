<?php

require dirname(__FILE__) . '/utils/KLogger.php';

// phpinfo();

$hostname = "localhost";
$dbname   = "video_switch";
$username = "video_switch";
$password = "";

date_default_timezone_set("Europe/Vatican"); # For KLogger not to show warnings
$log = new KLogger('/tmp/', KLogger::DEBUG); # Specify the log directory
$log->logInfo('control.php started'); //Prints to the log file

try {
    $dbh = new PDO("mysql:host=$hostname;dbname=$dbname", $username, $password);

    // getChannels: returns the channels data
    if(isset($_GET['getChannels'])) {
	$sql = "SELECT channels.id, channels.name, channels.uri, channel_types.chan_type FROM channels, channel_types " .
	       "WHERE channels.chan_type = channel_types.id AND channels.is_enabled = TRUE";

    $stmt = $dbh->query($sql);
	$chans = array();
	while($obj = $stmt->fetchObject()) {
            array_push($chans, $obj);
	}
	echo json_encode($chans);
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
