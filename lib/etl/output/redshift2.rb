require_relative '../redshift/client'

module ETL::Output

  # new Redshift outputter that defers logic to client
  class Redshift2 < Base
    include ETL::CachedLogger
    attr_reader :table_schema_lookup

    def initialize(client, table_schemas_lookup, transformer, validator, copy_options)
      super()
      @client = client
      @transformer = transformer
      @table_schemas_lookup = table_schemas_lookup
      @validator = validator
      @copy_options = copy_options

      raise "client cannot be nil" if @client.nil?
      raise "table_schemas_lookup cannot be nil" if @table_schemas_lookup.nil?
      raise "table_schemas_lookup cannot be empty" if @table_schemas_lookup.keys.count == 0
      raise "table_schemas_lookup must be a Hash" unless @table_schemas_lookup.is_a? Hash
    end

    # Runs the ETL job
    def run_internal
      rows_processed = @client.upsert_rows(reader, @table_schemas_lookup, @transformer, @validator, @copy_options)
      msg = "Processed #{rows_processed} input rows"
      ::ETL::Job::Result.success(rows_processed, msg)
    end
  end
end
