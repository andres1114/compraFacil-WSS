<?php
	
	function verbose($args) {
        switch ($args["outputMode"]) {
            case 0:
                echo "(".Date("Y-m-d H:i:s").") ".$args["outputMessage"]."\n";
                break;
            case 1:
                echo $args["outputMessage"];
                file_put_contents(realpath(__DIR__)."/".$args["logName"].".log", "(".Date("Y-m-d H:i:s").") ".$args["outputMessage"]."\n", FILE_APPEND);
                break;
        }
	}
	function doCurl($url,$headers,$rtype,$data) {//url, content type, request type,  data
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $rtype);
        curl_setopt($ch, CURLOPT_HTTPHEADER,$headers);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
?>