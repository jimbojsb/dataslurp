<?php
namespace Yak;

class Table
{
    /** @var  Connection */
    protected $connection;
    protected $name;
    protected $columns = [];
    protected $primaryKey = 'id';

    /**
     * Table constructor.
     * @param Connection $connection
     */
    public function __construct($name, Connection $connection)
    {
        $this->name = $name;
        $this->connection = $connection;
        $this->loadSchema();

    }

    public function insert(array $data)
    {
        $hasColumns = array_keys($data) != $data;

    }

    public function upsert($data, $columns = [])
    {
        if ($columns) {
            if (!in_array($this->primaryKey, $columns)) {
                throw new \RuntimeException("cannot upset without a primary key in the dataset");
            }
        } else {
            $columns = array_keys($this->columns);
        }

        foreach ($data as $row) {
            if (count($row) != count($columns)) {
                throw new \RuntimeException("column count does not match row count");
            }
            $columnString = implode("`,`", $columns);
            $quotedValues = [];
            foreach ($row as $val) {
                $quotedValues[] = $this->connection->quote($val);
            }
            $valueString = implode(",", $quotedValues);

            for ($c = 0; $c < count($quotedValues); $c++) {
                $key = $columns[$c];
                if ($key != $this->primaryKey) {
                    $val = $quotedValues[$c];
                    $updateStringParts[] = "$key=$val";
                }
            }
            $updateString = implode(", ", $updateStringParts);
            $sql = "INSERT INTO `$this->name` (`$columnString`) VALUES ($valueString)";
            $sql .= " ON DUPLICATE KEY UPDATE $updateString";

            $this->connection->getPdo()->query($sql);
        }
    }

    public function truncate()
    {

    }

    public function select()
    {

    }

    private function loadSchema()
    {
        $cols = $this->connection->getPdo()->query("DESCRIBE $this->name")->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($cols as $col) {
            if (isset($cols["Key"]) && $cols["Key"] == 'PRI') {
                $this->primaryKey = $cols["Field"];
            }
            $this->columns[$col["Field"]] = $col;
        }
    }
}