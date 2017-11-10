<?php
namespace DataSlurp;

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

    public function drop()
    {
        $this->connection->getPdo()->query("DROP TABLE $this->name");
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

    public function getColumns()
    {
        return $this->columns;
    }

    public function truncate()
    {
        $this->connection->getPdo()->query("TRUNCATE $this->name");
    }

    public function select($columns = "*", $where = null, $limit = null, $offset = null)
    {
        $sql = "SELECT ";
        if (is_string($columns)) {
            $sql .= $columns;
        } else if (is_array($columns)) {
            $sql .= "`" . implode("`,`", $columns) . "`";
        }
        $sql .= " FROM $this->name";
        $rows = $this->connection->getPdo()->query($sql)->fetchAll(\PDO::FETCH_ASSOC);
        return $rows;
    }

    public function bulkInsert(array $rows)
    {
        if (!$rows) {
            return;
        }

        $insertColumns = array_keys($rows[0]);
        $insertColumnsString = "(`" . implode("`,`", $insertColumns) . "`)";
        $baseInsertSql = "INSERT INTO $this->name $insertColumnsString VALUES ";
        $insertSql = $baseInsertSql;
        $expectedInsertCount = 0;

        foreach ($rows as $row) {
            foreach ($row as $key => $val) {
                if ($val === null) {
                    $row[$key] = 'NULL';
                } else {
                    $row[$key] = "'" . addslashes($val) . "'";
                }
            }
            $rowData = "(" . implode(",", array_values($row)) . ")";

            if (strlen($insertSql) + strlen($rowData) + 1 < $this->connection->getMaxPacket()) {
                $expectedInsertCount++;
                $insertSql .= $rowData . ",";
            } else {
                $this->runInsert($insertSql, $expectedInsertCount);

                $expectedInsertCount = 0;
                $expectedInsertCount++;
                $insertSql = $baseInsertSql;
                $insertSql .= $rowData . ",";
            }
        }
        unset($rows);

        if ($insertSql != $baseInsertSql) {
            $this->runInsert($insertSql, $expectedInsertCount);
        }
    }

    private function loadSchema()
    {
        $cols = $this->connection->getPdo()->query("DESCRIBE $this->name")->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($cols as $col) {
            if (isset($col["Key"]) && $col["Key"] == 'PRI') {
                $this->primaryKey = $col["Field"];
            }
            $this->columns[$col["Field"]] = $col['Type'];
        }
    }

    public function __toString()
    {
        return $this->name;
    }

    /**
     * @param $insertSql
     * @param $expectedInsertCount
     *
     * @return array
     * @throws \Exception
     */
    protected function runInsert($insertSql, $expectedInsertCount)
    {
        // remove extra comma
        $insertSql = rtrim($insertSql, ',');
        $pdo = $this->connection->getPdo();
        $result = $pdo->query($insertSql);
        if (!$result) {
            throw new \Exception(print_r($pdo->errorInfo(), true));
        }
        if ($result->rowCount() != $expectedInsertCount) {
            throw new \Exception("Expected to see $expectedInsertCount inserted, only saw " . $result->rowCount());
        }
    }


}