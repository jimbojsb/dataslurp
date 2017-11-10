<?php
namespace DataSlurp\Transfer;

use DataSlurp\Connection;

class CopyTransfer
{
    /** @var  Connection */
    protected $sourceConnection;

    /** @var  Connection */
    protected $destinationConnection;

    protected $config;

    protected $sourceTable;
    protected $destinationTable;
    protected $constraints;
    protected $transforms;
    protected $maps;
    protected $disableForeignKeys = true;
    protected $ignoreColumnDefinitionMismatch = true;

    public function __construct(Connection $sourceConnection, Connection $destinationConnection, array $config)
    {
        $this->sourceConnection = $sourceConnection;
        $this->destinationConnection = $destinationConnection;
        $this->config = $config;
        if (array_key_exists('chunk_size', $config) === true && is_integer($config['chunk_size']) === false ||
            array_key_exists('chunk_size', $config) === false) {
            $this->config['chunk_size'] = null;
        }
    }

    public function execute()
    {
        $this->destinationConnection->disableForeignKeyChecks();

        foreach ($this->config['tables'] as $table => $params) {

            // fix variables if the table has no params and is just a string
            if (is_int($table)) {
                $table = $this->config['tables'][$table];
                $params = [];
            }

            $sourceTable = $this->sourceConnection->getTable($table);
            if (!$sourceTable) {
                $this->sourceConnection->warning("$table not found on source. Skipping.");
                continue;
            }

            if (isset($params['destination_table'])) {
                $destinationTableName = $params['destination_table'];
            } else {
                $destinationTableName = $table;
            }

            $destinationTable = $this->destinationConnection->getTable($destinationTableName);
            if (!$destinationTable) {
                $this->destinationConnection->warning("$table not found on destination. Skipping.");
                continue;
            }

            $destinationTable->truncate();

            $sourceColumns = $sourceTable->getColumns();
            $destinationColumns = $destinationTable->getColumns();

            $columnsToSelect = [];
            foreach ($sourceColumns as $sourceColumnName => $sourceColumnDefinition) {
                if (isset($destinationColumns[$sourceColumnName])) {
                    $destinationColumnDefinition = $destinationColumns[$sourceColumnName];
                    if ($sourceColumnDefinition != $destinationColumns[$sourceColumnName]) {
                        $this->destinationConnection->warning("$table.$sourceColumnName definition mismatch: source is $sourceColumnDefinition, destination is $destinationColumnDefinition");
                    }
                    $columnsToSelect[] = $sourceColumnName;
                } else {
                    $this->destinationConnection->warning("$table.$sourceColumnName: column missing on destination");
                }
            }

            $limit = $this->config['chunk_size'];
            if ($limit !== null) {
                $offset = 0;
                while ($sourceRows = $sourceTable->select($columnsToSelect, $limit, $offset)) {
                    $destinationTable->bulkInsert($sourceRows);
                    $offset += count($sourceRows);
                }
            } else {
                $sourceRows = $sourceTable->select($columnsToSelect);
                $destinationTable->bulkInsert($sourceRows);
            }
        }
    }
}