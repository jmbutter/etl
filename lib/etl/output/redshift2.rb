require 'tempfile'
require 'aws-sdk'
require 'csv'
require_relative '../redshift/client'
require_relative '../redshift/table'

module ETL::Output

  # Class that contains shared logic for loading data from S3 to Redshift.
  # Moving forward this class will be used. Compared to the previous base
  # it will not ever modify or create the schema of the output table
  # that will be the job of the schema migration instead. It will allow
  # row level writes to multiple tables.
  class Redshift2 < Base

    def initialize(client, dest_table)
      super()
      @dest_table = dest_table
      @client = client
    end

    def default_schema
      @client.table_schema(@dest_table)
    end

    # Name of the destination table. By default we assume this is the class
    # name but you can override this in the parameters.
    def dest_table
      @dest_table ||
        ETL::StringUtil::camel_to_snake(ETL::StringUtil::base_class_name(self.class.name))
    end

    # Runs the ETL job
    def run_internal
      @client.upsert_rows(reader, @dest_table)
      msg = "Processed #{rows_processed} input rows for #{dest_table}"
      ETL::Job::Result.success(rows_processed, msg)
    end

  end
end

