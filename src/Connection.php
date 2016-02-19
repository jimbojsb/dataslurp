<?php
namespace DataSlurp;

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerTrait;

class Connection implements LoggerAwareInterface
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /** @var  \PDO */
    protected $pdo;

    protected $maxPacket;
    protected $databaseName;

    /**
     * Connection constructor.
     * @param \PDO $pdo
     */
    public function __construct(\PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    public function getDatabaseName()
    {
        if (!isset($this->databaseName)) {
            $this->databaseName = $this->pdo->query("SELECT DATABASE()")->fetchColumn();
        }
        return $this->databaseName;
    }

    public function disableForeignKeyChecks()
    {
        $this->pdo->query("SET FOREIGN_KEY_CHECKS=0");
    }

    public function getMaxPacket()
    {
        if (!isset($this->maxPacket)) {
            $this->maxPacket = (int) $this->pdo->query("SHOW VARIABLES LIKE 'max_allowed_packet'")->fetchColumn(1);
        }
        return $this->maxPacket;
    }

    public function getTable($tableName)
    {
        try {
            return new Table($tableName, $this);
        } catch (\PDOException $e) {
            return false;
        }
    }

    public function getTables()
    {
        $tables = [];
        $result = $this->pdo->query("SHOW TABLES");
        while ($table = $result->fetch(\PDO::FETCH_COLUMN)) {
            $tables[] = new Table($table, $this);
        }
        return $tables;
    }

    public function quote($string)
    {
        return $this->pdo->quote($string);
    }

    public function getPdo()
    {
        return $this->pdo;
    }

    public function log($level, $message, array $context = array())
    {
        $this->logger->log($level, $message, $context);
    }


}