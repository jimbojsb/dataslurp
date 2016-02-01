<?php
namespace DataSlurp;

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Psr\Log\LoggerInterface;

class Connection implements LoggerAwareInterface
{
    use LoggerAwareTrait;

    /** @var  \PDO */
    protected $pdo;

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
        return $this->pdo->query("SELECT DATABASE()")->fetchColumn();
    }

    public function getTable($tableName)
    {
        return new Table($tableName, $this);
    }

    public function quote($string)
    {
        return $this->pdo->quote($string);
    }

    public function getPdo()
    {
        return $this->pdo;
    }
}