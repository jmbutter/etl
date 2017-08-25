require_relative '../redshift/client'
require_relative '../redshift/table'
require_relative '../redshift/date_table_id_augmenter'
require_relative '../redshift/id_augmenter'

module ETL::Output

  # new Redshift outputter that defers logic to client
  class Redshift2 < Base
    include ETL::CachedLogger

    def initialize(client, data_table_name, history_table_name, surrogate_key, natural_keys, scd_columns, delimiter='|', id_generator)
      super()
      @id_generator = id_generator
      @delimiter = delimiter
      @client = client
      @surrogate_key = surrogate_key
      @natural_keys = natural_keys
      @data_table_name = data_table_name
      @history_table_name = history_table_name
    end

    # Runs the ETL job
    def run_internal
      row_transformers = []
      row_splitter = nil
      table_schemas = []
      data_table_schema = @client.table_schema(data_table_name)
      history_table_schema = @client.table_schema(history_table_name)
      transformer = ::ETL::Redshift.DataHistoryRowTransformer.new(@scd_columns, @id_generator, data_table_schema, history_table_schema)

      rows_processed = @client.upsert_rows(reader, [data_table_schema, history_table_schema], transformer)
      msg = "Processed #{rows_processed} input rows for #{@destination_tables.join(",")}"
      ::ETL::Job::Result.success(rows_processed, msg)
    end
  end

  class DataHistoryRowTransformer
    def initialize(scd_columns, id_generator, data_table_schema, history_table_schema)
      @scd_columns = scd_columns
      @id_generator = id_generator
      @data_table_schema = data_table_schema
      @history_table_schema = history_table_schema
    end
    def transform(row)
      named_rows = {}
      # Conditionally creates more named rows based on the
      # change in the row
      id_augmenter = ::ETL::Redshift::DateTableIDAugmenter.new([data_table_schema, history_table_schema])
      row = id_augmenter.transform(row)
      row_splitter = ::ETL::Transform::SplitRow.SplitByTableSchemas([data_table_schema, history_table_schema])
      named_rows = row_splitter.transform(row)

      # If its a new row in the orgs table then
      #    a. new row for orgs
      #    b. new row for orgs_history
      data_table_surrogate_key = data_table_schema.primary_key if data_table_schema.primary_key.count > 1 raise "data table #{data_table_schema.name} must only have one pk"
      # If no surrogate key exists then none was found so its new.
      if !row.has_key?(data_table_surrogate_key)
        new_history_id = id_generator.generate_id
        new_surrogate_id = id_generator.generate_id
        history_table_surrogate_key = history_table_schema.primary_key if history_table_schema.primary_key.count > 1 raise "history table #{history_table_schema.name} must only have one pk"
        row[data_table_surrogate_key] = new_surrogate_id
        row[history_table_surrogate_key] = new_history_id
        named_rows[history_table_schema.name]["current"] = true
        named_rows[history_table_schema.name]["created_at"] = DateTime.now.utc
      else
        scd_changed = false
        @scd_columns.each do |scd|
          if row["old_#{scd}"] != row[scd]
            scd_changed = true
            break
          end
        end
        if scd_changed
            # If its an existing row in the orgs table scd column that changed
            #    a. update current orgs_history row to not current and add end_date
            #    b. add new row for orgs_history
            new_history_row = existing_history_row.clone
            existing_history_row = named_rows[history_table_schema.name]
            existing_history_row["current"] = false
            existing_history_row["ended_at"] = DateTime.now.utc

            new_history_id = id_generator.generate_id
            new_history_row["current"] = true
            new_history_row["started_at"] = DateTime.now.utc
            named_rows[history_table_schema.name] = [existing_history_row, new_history_row]
        end

        named_rows
      end
    end
  end
end
