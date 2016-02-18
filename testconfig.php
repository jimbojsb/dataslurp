<?php
$config = [
    "tables" => [
        "raw_cards"
    ]
];

require_once __DIR__ . '/vendor/autoload.php';

$sourcePdo = new \PDO("mysql:charset=utf8;dbname=quint;host=aurora.quint.ahab", "quint", "GS~2%%aV5!3W:AH");
$destinationPdo = new \PDO("mysql:charset=utf8;dbname=quint;host=127.0.0.1;port=20030", "quint", "quint");

$logger = new Monolog\Logger("test");

$sourceConnection = new \DataSlurp\Connection($sourcePdo);
$destinationConnection = new \DataSlurp\Connection($destinationPdo);
//$sourceConnection->setLogger($logger);
//$destinationConnection->setLogger($logger);


$copyTransfer = new \DataSlurp\Transfer\CopyTransfer($sourceConnection, $destinationConnection, $config);
$copyTransfer->execute();