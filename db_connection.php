<?php	

//=============================================================

//This script is in charge of creating a connection to a postgres database server
//using PHP's PDO and dealing with the possible errors during query runtime

//=============================================================

    error_reporting(E_ALL);
    ini_set('display_errors', 'On');

    function pdoCreateConnection($args) {
        //Define the required and regular parameters to start a DB connection
        $db_host = $args['db_host'];
        $db_user = $args['db_user'];
        $db_pass = $args['db_pass'];
        $db_name = $args['db_name'];
        $db_type = $args['db_type'];
        $db_charset = "utf8mb4";

        //Define the PDO's DSN and its options
        switch ($db_type) {
            case "postgres":
                $pdo_dsn = "pgsql:host=$db_host;dbname=$db_name;";
                $pdo_opt = array(
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                    ,PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
                    ,PDO::ATTR_EMULATE_PREPARES => false
                );
                break;
            case "mysql":
                $pdo_dsn = "mysql:host=$db_host;dbname=$db_name;charset=$db_charset";
                $pdo_opt = array(
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_EMULATE_PREPARES => false,
                    PDO::MYSQL_ATTR_INIT_COMMAND => "SET character_set_results = 'utf8', character_set_client = 'utf8', character_set_connection = 'utf8', character_set_database = 'utf8', character_set_server = 'utf8'"
                );
                break;
            case "sqlite":
                $pdo_dsn = "sqlite:$db_host";
                $pdo_opt = array(
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                    ,PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
                    ,PDO::ATTR_EMULATE_PREPARES => false
                );
                break;
            default:
                $pdo_dsn = "pgsql:host=$db_host;dbname=$db_name;";
                $pdo_opt = array(
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                    ,PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
                    ,PDO::ATTR_EMULATE_PREPARES => false
                );
                break;
        }

        //Start the DB connection or return an exception on failure
        try {
            $pdo = new PDO($pdo_dsn, $db_user, $db_pass, $pdo_opt);
        } catch (PDOException $e) {
            throw new RuntimeException("Caught exception: " . $e->getMessage() . "on script ".basename($_SERVER['PHP_SELF'])." and query reference: PDO_OBJECT_CREATION");
        }

        return $pdo;
    }

	//Define the query execution function through PDO or return an exception on failure, with the arguments as:
	//$pdo = 			The PDO DB connection object
	//$qry = 			The query to execute
	//$qry_args = 		The arguments array passed to the query using PDO's anti SQLInjection filtering system
	//$qry_reference = 	The query reference to track back when an error happens
	function pdoExecuteQuery($pdo,$qry,$qry_args,$qry_reference) {
		
		//Attempt to start a transaction to the database or return an exception on failure
		try {
			$pdo->beginTransaction();
		} catch (PDOException $e) {
			throw new RuntimeException("Caught exception: ".$e->getMessage().", on script ".basename($_SERVER['PHP_SELF'])." and query reference: ".$qry_reference."_BEGIN_TRANSACTION");
			
			//Return to a previous state before the failure
			$pdo->rollBack();
			
			//End the connection to the DB
			$pdo = NULL;
			
			//Close the script
			exit;				
		}
		
		//Attempt to prepare the started transaction
		try {
			$pdo_prepared = $pdo->prepare($qry);
		} catch (PDOException $e) {
			throw new RuntimeException("Caught exception: ".$e->getMessage().", on script ".basename($_SERVER['PHP_SELF'])." and query reference: ".$qry_reference."_TRANSCATION_PREPARE");
			
			//Return to a previous state before the failure
			$pdo->rollBack();
			
			//End the connection to the DB
			$pdo = NULL;
			
			//Close the script
			exit;			
		}
		
		//Attempt to bind the parameters to using PDO's anti SQLInjection filtering system or return an exception on failure
		try {
			
			//Discompose the $qry_args associative array into array keys and array values
			$arrayKeys = array_keys($qry_args);
			$arrayValues = array_values($qry_args);
			
			//Loop through the array values to bind every value or null to the query, where bindValues arguments are as follows:
			//bindValue($argument1,$argument2,PDO::PARAM_TYPE)
			//$argument1 = 		the array key
			//$argument2 = 		the array value
			//PDO::PARM_TYPE = 	the type of value
			for ($arrayCounter = 0; $arrayCounter < sizeof($arrayKeys); $arrayCounter++) {
				if ($arrayValues[$arrayCounter] === null) {
                    $pdo_prepared->bindValue(":" . $arrayKeys[$arrayCounter], null, PDO::PARAM_INT);
                } else if (is_bool($arrayValues[$arrayCounter])) {
                    $pdo_prepared->bindValue(":".$arrayKeys[$arrayCounter],$arrayValues[$arrayCounter],PDO::PARAM_BOOL);
				} else {
					$pdo_prepared->bindValue(":".$arrayKeys[$arrayCounter],$arrayValues[$arrayCounter],PDO::PARAM_STR);
				}
			}
			
		} catch(PDOException $e) {
			throw new RuntimeException("Caught exception: ".$e->getMessage().", on script ".basename($_SERVER['PHP_SELF'])." and query reference: ".$qry_reference."_TRANSCATION_BIND_VALUES");
			
			//Return to a previous state before the failure
			$pdo->rollBack();
			
			//End the connection to the DB
			$pdo = NULL;
			
			//Close the script
			exit;					
		}
		
		//Attempt to execute the query or return an exception on failure
		try {			
			$pdo_prepared->execute();
		} catch (PDOException $e) {
			throw new RuntimeException("Caught exception: ".$e->getMessage().", on script ".basename($_SERVER['PHP_SELF'])." and query reference: ".$qry_reference."_TRANSCATION_EXECUTE");
			
			//Return to a previous state before the failure
			$pdo->rollBack();
			
			//End the connection to the DB
			$pdo = NULL;
			
			//Close the script
			exit;		
		}
		
		//Attempt to get the last inserted ID in case of an INSERT query or return an exception on failure
        if (strpos(strtolower($qry), "insert") !== false) {
            try {
                $lastSuccessfullId = $pdo->lastInsertId();
            } catch (PDOException $e) {
                throw new RuntimeException("Caught exception: " . $e->getMessage() . ", on script " . basename($_SERVER['PHP_SELF']) . " and query reference: " . $qry_reference . "_TRANSCATION_GET_LAST_ID");

                //Return to a previous state before the failure
                $pdo->rollBack();

                //End the connection to the DB
                $pdo = NULL;

                //Close the script
                exit;
            }
        } else {
            $lastSuccessfullId = NULL;
        }
		
		//Attempt to commit the transaction or return an exception on failure
		try {
			$pdo->commit();
		} catch (PDOException $e) {
			throw new RuntimeException("Caught exception: ".$e->getMessage().", on script ".basename($_SERVER['PHP_SELF'])." and query reference: ".$qry_reference."_TRANSCATION_COMMIT");
			
			//Return to a previous state before the failure
			$pdo->rollBack();
			
			//End the connection to the DB
			$pdo = NULL;
			
			//Close the script
			exit;
		}

		//This part of code is to make the queries compatible with SQLite connection types
        $pdoFetched = $pdo_prepared->fetchAll();
        $pdoRowCount = sizeof($pdoFetched);

		//Return an array with the next information:
		//[0] = The associative array containing the returned values of a SELECT query
		//[1] = The amount of rowS returned by the query
		//[2] = The query used for debugging or logging purposes
		//[3] = The returned ID 
		return array($pdoFetched,$pdoRowCount,interpolateQuery($qry,$qry_args),$lastSuccessfullId);
		
	}	
	
	//Define a simple special character filtering function
	function clean($string) {
		$string = stripcslashes(str_replace(array('<', '>', '&', '{', '}', '[', ']','"',"'"), array(''), $string));		
		return $string;
	}
	
	//Define a debugging function to show the parsed query and arguments to the PDO, with the arguments as:
	//$qry = 		The query used in the PDO
	//$params = 	The arguments that will be bound to the query
	function interpolateQuery($query, $params) {

        $arrayKeys = array_keys($params);
        $arrayValues = array_values($params);

        $keys = array();
        $values = array();

		for ($x = 0; $x < sizeof($arrayKeys); $x++) {
            $keys[$x] = "/:".$arrayKeys[$x]."/";
            $values[$x] = "'".$arrayValues[$x]."'";
		}

		$query = preg_replace($keys, $values, $query, 1, $count);
		return $query;
	}

	//Definte the function to convert an associative array into a regular array
    function cast_assoc_array_to_array($key_to_convert,$associative_array) {
        $retun_array = array();

        for ($x = 0; $x < sizeof($associative_array); $x++) {
            $retun_array[$x] = $associative_array[$x][$key_to_convert];
        }

        return $retun_array;
    }

    //Define the function to log every INSERT / UPDATE / DELETE action by the user
    function logHandler($args) {

        $pdo_log_db = pdoCreateConnection(array('db_type' => "sqlite", 'db_host' => realpath(__DIR__).'/log_db.sqlite3', 'db_user' => "root", 'db_pass' => "", 'db_name' => ""));

        $dbtablecolumns = array(
            "user_id"
            ,"user_name"
            ,"timestamp"
            ,"action"
            ,"query"
        );

        switch ($args["action"]) {
            case "insert":
                $qry_args = array(
                    "userid" => USER_ID
                    ,"username" => USER_NAME
                    ,"timestamp" => SERVER_TIME
                    ,"logaction" => "DB_INSERT_DATA"
                    ,"logquery" => $args["query"]
                );
                $qry = "INSERT INTO log (".implode(",",$dbtablecolumns).") VALUES (:userid,:username,:timestamp,:logaction,:logquery)";
                pdoExecuteQuery($pdo_log_db,$qry,$qry_args,"log_handler_insert");

                break;
            case "update":
                $qry_args = array(
                    "userid" => USER_ID
                    ,"username" => USER_NAME
                    ,"timestamp" => SERVER_TIME
                    ,"logaction" => "DB_UPDATE_DATA"
                    ,"logquery" => $args["query"]
                );
                $qry = "INSERT INTO log (".implode(",",$dbtablecolumns).") VALUES (:userid,:username,:timestamp,:logaction,:logquery)";
                pdoExecuteQuery($pdo_log_db,$qry,$qry_args,"log_handler_update");

                break;
            case "delete":

                $qry_args = array(
                    "userid" => USER_ID
                    ,"username" => USER_NAME
                    ,"timestamp" => SERVER_TIME
                    ,"logaction" => "DB_DELETE_DATA"
                    ,"logquery" => $args["query"]
                );
                $qry = "INSERT INTO log (".implode(",",$dbtablecolumns).") VALUES (:userid,:username,:timestamp,:logaction,:logquery)";
                pdoExecuteQuery($pdo_log_db,$qry,$qry_args,"log_handler_delete");

                break;
        }

        $qry_args = array(
            "timestamp" => SERVER_TIME
        );
        $qry = "DELETE FROM log WHERE timestamp < date(:timestamp, '-120 day')";
        pdoExecuteQuery($pdo_log_db,$qry,$qry_args,"log_handler_clear");
    }