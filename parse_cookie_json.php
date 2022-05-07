<?php
    error_reporting(E_ALL);
    ini_set('display_errors', 'On');
    date_default_timezone_set('America/New_York');

    //Include the db connection file
    include_once "db_connection.php";
    //Include the functions file
    include_once "functions.php";

    verbose(array("outputMode" => 0, "outputMessage" => "Starting the cookie parser script", "logName" => "parse_cookie_json"));

    verbose(array("outputMode" => 0, "outputMessage" => "Connecting to the eprensa database...", "logName" => "parse_cookie_json"));
    //Create the PDO connection objects
    $pdo_mysql = pdoCreateConnection(array('db_type' => "mysql", 'db_host' => "softwareaez.lol", 'db_user' => "softwaez_comprafacil", 'db_pass' => "BDme2Ne}Pll-", 'db_name' => "softwaez_comprafacil"));
    verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));

    //Define the constants
    $current_script_path = realpath(__DIR__)."/";
    $json_folder = "cookie_json_files";
    $ignored_files = array('.', '..');

    verbose(array("outputMode" => 0, "outputMessage" => "Processing folder '$json_folder' in path '$current_script_path'" , "logName" => "parse_cookie_json"));
    //Check for json files
    $json_listing = scandir($current_script_path.$json_folder,SCANDIR_SORT_ASCENDING);

    //Loop through the found files
    for ($x = 0; $x < sizeof($json_listing); $x++) {
        $json_file_name = $json_listing[$x];

        if (!in_array($json_file_name, $ignored_files) && is_file($current_script_path.$json_folder."/".$json_file_name)) {
            verbose(array("outputMode" => 0, "outputMessage" => "Processing file '".$json_file_name."' in path '".$current_script_path.$json_folder."'", "logName" => "parse_cookie_json"));

            if(strpos($json_file_name,'.json') === false) {
                verbose(array("outputMode" => 0, "outputMessage" => "File '".$json_file_name."' does not have a json extension, skipping", "logName" => "parse_cookie_json"));
                continue;
            }

            $domain = preg_replace("/\.json/","", $json_file_name);
			if (is_numeric($domain)) {
				$domain_id = $domain;
			} else {
				//Get the domain id
				$query_args = array(
					"domain" => $domain
				);
				$query = "SELECT id FROM almacen WHERE nombre_almacen = :domain";
				$query_data = pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_01");
	
				if ($query_data[1] == 0) {
					verbose(array("outputMode" => 0, "outputMessage" => "The value '".$domain."' did not return any domain_id, skipping", "logName" => "parse_cookie_json"));
					continue;
				} else {
					$domain_id = $query_data[0][0]["id"];
				}
			}

            //Get the json's content
            $json_content = json_decode(file_get_contents($current_script_path.$json_folder."/".$json_file_name));

            for ($y = 0; $y < sizeof($json_content); $y++) {
                $cookie_name = $json_content[$y]->name;
                $cookie_value = $json_content[$y]->value;

                verbose(array("outputMode" => 0, "outputMessage" => "Processing cookie name '$cookie_name' for domain '$domain' (ID $domain_id)", "logName" => "parse_cookie_json"));

                verbose(array("outputMode" => 0, "outputMessage" => "Checking whether the cookie already exists in the table scrapy_headers", "logName" => "parse_cookie_json"));
                $query_args = array(
                    "name" => $cookie_name
                );
                $query = "SELECT id, header_name, header_value FROM scrapy_headers WHERE header_name = :name";
                $query_data = pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_02");

                if ($query_data[1] > 0) {
                    verbose(array("outputMode" => 0, "outputMessage" => "The cookie existes, checking whether it needs an update", "logName" => "parse_cookie_json"));
                    $cookie_id = $query_data[0][0]["id"];

                    if ($query_data[0][0]["header_value"] != $cookie_value) {
                        verbose(array("outputMode" => 0, "outputMessage" => "The cookie needs an update, updating...", "logName" => "parse_cookie_json"));

                        $query_args = array(
                            "cookieid" => $cookie_id
                            ,"value" => $cookie_value
                            ,"status" => 'Active'
                        );
                        $query = "UPDATE scrapy_headers SET header_value = :value, header_status = :status, active = TRUE WHERE id = :cookieid";
                        pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_03");
                        verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));

                    } else {
                        verbose(array("outputMode" => 0, "outputMessage" => "No update needed, activating the cookie...", "logName" => "parse_cookie_json"));

                        $query_args = array(
                            "cookieid" => $cookie_id
                            ,"status" => 'Active'
                        );
                        $query = "UPDATE scrapy_headers SET header_status = :status, active = TRUE WHERE id = :cookieid";
                        pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_04");
                        verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));
                    }
                } else {
                    verbose(array("outputMode" => 0, "outputMessage" => "The cookie does not exists, inserting...", "logName" => "parse_cookie_json"));

                    $query_args = array(
                        "domainid" => $domain_id
                        ,"type" => 1
                        ,"name" => $cookie_name
                        ,"value" => $cookie_value
                        ,"status" => 'Active'
                    );
                    $query = "INSERT INTO scrapy_headers (domain_id, header_type, header_name, header_value, header_status, days_until_expiration, active) VALUES (:domainid, :type, :name, :value, :status, NULL, TRUE)";
                    pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_05");
                    verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));
                }
            }

            verbose(array("outputMode" => 0, "outputMessage" => "Deleting the un-used cookies...", "logName" => "parse_cookie_json"));
            $query_args = array(
                "domainid" => $domain_id
                ,"type" => 1
                ,"status" => 'ERROR: cookies have expired'
            );
            $query = "DELETE FROM scrapy_headers WHERE domain_id = :domainid AND header_type = :type AND header_status = :status";
            pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_06");
            verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));

            verbose(array("outputMode" => 0, "outputMessage" => "Deleting the file '$json_file_name' in path '".$current_script_path.$json_folder."'", "logName" => "parse_cookie_json"));
            unlink($current_script_path.$json_folder."/".$json_file_name);
            verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));

            verbose(array("outputMode" => 0, "outputMessage" => "Cookie process done", "logName" => "parse_cookie_json"));
            verbose(array("outputMode" => 0, "outputMessage" => "Activating the spider '$domain' (ID $domain_id)...", "logName" => "parse_cookie_json"));

            $query_args = array(
                "domainid" => $domain_id
                ,"status" => 'Active'
            );
            $query = "UPDATE scrapy_spiders SET spider_status = :status, active = TRUE WHERE domain_id = :domainid";
            pdoExecuteQuery($pdo_mysql,$query,$query_args,"query_07");

            verbose(array("outputMode" => 0, "outputMessage" => "Done", "logName" => "parse_cookie_json"));
        }
    }
    verbose(array("outputMode" => 0, "outputMessage" => "Script done, exiting", "logName" => "parse_cookie_json"));

?>