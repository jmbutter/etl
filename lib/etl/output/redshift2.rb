require_relative '../redshift/client'
require_relative '../redshift/table'
require_relative '../redshift/date_table_id_augmenter'
require_relative '../redshift/id_augmenter'

module ETL::Output

  # new Redshift outputter that defers logic to client
  class Redshift2 < Base
    include ETL::CachedLogger

    def initialize(client, destination_tables, surrogate_key, natural_keys, delimiter='|', id_generator)
      super()
      @id_generator = id_generator
      @delimiter = delimiter
      @client = client
      @surrogate_key = surrogate_key
      @natural_keys = natural_keys
      @destination_tables = destination_tables
    end

    # Runs the ETL job
    def run_internal
      row_transformers = []
      row_splitter = nil
      schemas = []
      @destination_tables.each do |t|
        table_schema = @client.table_schema(t)
        row_transformers << ::ETL::Redshift::DateTableIDAugmenter.new(table_schema)
        row_transformers << ::ETL::Redshift::IDAugmenter.new(@client, table_schema, @natural_keys, nil, @id_generator) if table_schema.has_keys?(@natural_keys)
        schemas << table_schema
      end
      if schemas.count > 1
        row_splitter = ::ETL::Transform::SplitRow.SplitByTableSchemas(schemas)
      end

      rows_processed = @client.upsert_rows(reader, @destination_tables, row_splitter, row_transformers)
      msg = "Processed #{rows_processed} input rows for #{@destination_tables.join(",")}"
      ::ETL::Job::Result.success(rows_processed, msg)
    end
  end
end
