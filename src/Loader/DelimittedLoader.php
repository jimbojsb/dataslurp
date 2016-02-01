<?php
namespace Yak\Loader;

use Yak\Connection;

class DelimittedLoader
{
    protected $delimitter;
    protected $sourceFile;
    protected $columnsFromFirstLine = true;
    protected $tableName;
    protected $upsert;

    /** @var  Connection */
    protected $connection;

    /**
     * DelimittedLoader constructor.
     * @param $delimitter
     */
    public function __construct(Connection $connection, $tableName, $sourceFile, $delimitter, $upsert = true)
    {
        $this->sourceFile = $sourceFile;
        $this->delimitter = $delimitter;
        $this->tableName = $tableName;
        $this->upsert = $upsert;
        $this->connection = $connection;
    }

    /**
     * @param boolean $columnsFromFirstLine
     */
    public function setColumnsFromFirstLine($columnsFromFirstLine)
    {
        $this->columnsFromFirstLine = $columnsFromFirstLine;
    }

    public function execute()
    {
        $fileIterator = new \SplFileObject($this->sourceFile);
        $fileIterator->setCsvControl($this->delimitter);
        $fileIterator->setFlags(\SplFileObject::READ_CSV | \SplFileObject::DROP_NEW_LINE);

        $columns = [];
        $data = [];
        foreach ($fileIterator as $values) {
            $row = [];
            foreach ($values as $val) {
                $row[] = trim($val);
            }
            $data[] = $row;
        }

        if ($this->columnsFromFirstLine) {
            $columns = array_shift($data);
        }

        $this->connection->getTable($this->tableName)->upsert($data, $columns);

    }
}