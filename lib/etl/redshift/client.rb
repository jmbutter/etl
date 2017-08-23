require 'tempfile'
require 'aws-sdk'
require 'csv'
# removing due to ubuntu 14.04 deployment issues
#require 'odbc'
require 'mixins/cached_logger'
require 'pg'
require 'pathname'

module ETL::Redshift

  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME="Amazon Redshift (x64)"

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :db, :region, :iam_role, :bucket

    # when odbc driver is fully working the use redshift driver can
    # default to true
    def initialize(conn_params={}, aws_params={})
      @use_redshift_odbc_driver = false
      @conn_params = conn_params
      @region = aws_params.fetch(:region, '')
      @iam_role = aws_params.fetch(:role_arn, '')
      @bucket = aws_params.fetch(:s3_bucket, '')
      @random_key = [*('a'..'z'),*('0'..'9')].shuffle[0,10].join
      ObjectSpace.define_finalizer(self, proc { db.disconnect })
    end

    def db
      @db ||= begin
                PG.connect(@conn_params)
# removing due to ubuntu 14.04 deployment issues
#                if @use_redshift_odbc_driver then
#                  Sequel.odbc(@conn_params)
#                else
#                  Sequel.postgres(@conn_params)
#                end
              end
    end

    def execute(sql)
      puts "#{sql}\n"
      log.debug("SQL: '#{sql}'")
      db.exec(sql)
    end

    def drop_table(table_name)
      sql = "drop table if exists #{table_name};"
      execute(sql)
    end

    def create_table(table)
      sql = table.create_table_sql(@use_redshift_odbc_driver)
      execute(sql)
    end

    def table_schema(table_name)
      information_schema_columns_sql = <<SQL
select i.column_name, i.table_name, i.ordinal_position, i.is_nullable, i.data_type, i.character_maximum_length, i.numeric_precision, i.numeric_precision_radix, i.numeric_scale, i.udt_name, pg_table_def.distkey, pg_table_def.sortkey
from information_schema.columns as i left outer join pg_table_def
      on pg_table_def.tablename = i.table_name and i.column_name = pg_table_def.\"column\" where i.table_name = '#{table_name}'
SQL
      columns_info = execute(information_schema_columns_sql)

      table_constraint_info_sql = <<SQL
      SELECT conkey
      FROM pg_constraint
      WHERE contype = 'p' and conrelid = (
          SELECT oid FROM pg_class WHERE relname LIKE '#{table_name}');
SQL
      pk_ordinals = []
      values = execute(table_constraint_info_sql).values
      if !values.nil? && values.length > 0
        values[0][0].tr('{}', '').split(",").each do |v|
          pk_ordinals << v.to_i
        end
      end
      ::ETL::Redshift::Table.from_schema(table_name, columns_info, pk_ordinals)
    end

    def columns(table_name)
      sql = <<SQL
      SELECT "column", type FROM pg_table_def WHERE tablename = '#{table_name}'
SQL
      execute(sql)
    end

    def count_row_by_s3(destination)
      sql = <<SQL
        SELECT c.lines_scanned FROM stl_load_commits c, stl_query q WHERE filename LIKE 's3://#{destination}%'
        AND c.query = q.query AND trim(q.querytxt) NOT LIKE 'COPY ANALYZE%'
SQL
      results = execute(sql)
      loaded_rows = 0
      results.each { |result| loaded_rows += result.fetch("lines_scanned", "0").to_i }
      loaded_rows
    end

    def unload_to_s3(query, destination, delimiter = '|')
      sql = <<SQL
        UNLOAD ('#{query}') TO 's3://#{destination}'
        IAM_ROLE '#{@iam_role}'
        DELIMITER '#{delimiter}'
SQL
      execute(sql)
    end

    def copy_from_s3(table_name, destination, delimiter = '|')
      sql = <<SQL
        COPY #{table_name}
        FROM 's3://#{destination}'
        IAM_ROLE '#{@iam_role}'
        TIMEFORMAT AS 'auto'
        DATEFORMAT AS 'auto'
        ESCAPE
        DELIMITER '#{delimiter}'
        REGION '#{@region}'
SQL
      execute(sql)
    end

    def delete_object_from_s3(bucket, prefix, session_name)
      creds = ::ETL.create_aws_credentials(@region, @iam_role, session_name)

      s3 = Aws::S3::Client.new(region: @region, credentials: creds)
      resp = s3.list_objects(bucket: bucket)
      keys = resp[:contents].select { |content| content.key.start_with? prefix }.map { |content| content.key }

      keys.each { |key| s3.delete_object(bucket: bucket, key: key) }
    end

    # Upserts rows into the destintation tables based on rows
    # provided by the reader.
    def upsert_rows(reader, destination_tables, row_splitter = nil, row_transformers = [], delimiter='|')
      # write csv files
      arr = write_csv_files(reader, destination_tables, row_splitter, row_transformers, delimiter)
      rows_processed = arr[0]
      file_paths = arr[1]
      table_schemas = arr[2]

      if rows_processed == 0
        log.warn("No rows processed")
        return rows_processed
      end

      tmp_session = destination_tables.join("_") + @random_key

      # upload files to s3
      creds = ::ETL.create_aws_credentials(@region, @iam_role, tmp_session)
      s3_file_names = {}
      file_paths.each do |t, fp|
        s3_file_names[t] = File.basename(fp)
        s3_resource = Aws::S3::Resource.new(region: @region, credentials: creds)
        s3_resource.bucket(@bucket).object(s3_file_names[t]).upload_file(fp)
      end

      destination_tables.each do |t|
        table_schema = table_schemas[t]
        tmp_table = create_staging_table(t)
        pks = table_schema.primary_key
        s3_path = "#{@bucket}/#{s3_file_names[t]}"

        copy_from_s3(tmp_table, s3_path, delimiter)
        delete_sql = <<SQL
          DELETE FROM #{t}
          USING #{tmp_table} s
          WHERE #{pks.collect{ |pk| "#{t}.#{pk} = s.#{pk}" }.join(" and ")}
SQL
        execute(delete_sql)

        # load temporary tables
        insert_sql = <<SQL
          INSERT INTO #{t}
          SELECT * FROM #{tmp_table}
SQL
        execute(insert_sql)
      end
      rows_processed
    end

    def create_staging_table(destination_table)
      tmp_table_name = destination_table + @random_key
      # create temp table to add data to.
      tmp_table = ::ETL::Redshift::Table.new(tmp_table_name, { temp: true, like: destination_table })
      create_table(tmp_table)
      tmp_table_name
    end

    def write_csv_files(reader, destination_tables, row_splitter = nil, row_transformers = [], delimiter = '|')
      # Remove new lines ensures that all row values have newlines removed.
      row_transformers << ::ETL::Transform::RemoveNewlines.new
      table_schemas = {}
      csv_files = {}
      csv_file_paths = {}
      destination_tables.each do |t|
        csv_file_paths[t] = Tempfile.new(t)
        csv_files[t] = ::CSV.open(csv_file_paths[t], "w", {:col_sep => delimiter } )
        table_schemas[t] = table_schema(t)

        # Initialize the headers in csvs
        raise "Table '#{t}' does not have a primary key" unless table_schemas[t].primary_key.count > 0
      end
      
      rows_processed = 0
      begin
        reader.each_row do |row|
          row_transformers.each do |tf|
            row = tf.transform(row)
          end
          if !row_splitter.nil?
            rows = row_splitter.transform(row)
          else
            raise "Expected only 1 destination table as no row splitter is used" if destination_tables.count > 1
            rows = {destination_tables[0] => row}
          end
          rows.each do |key, split_row|
            table_schema = table_schemas[key]

            values_arr = []
            table_schema.columns.keys.each do |c|
             if split_row.has_key?(c)
                values_arr << split_row[c]
              else
                values_arr << nil
              end
            end
            csv_row = CSV::Row.new(table_schema.columns.keys, values_arr)
            csv_files[key].add_row(csv_row)
          end
          rows_processed += 1
        end
      ensure
        destination_tables.each do |t|
          csv_files[t].close()
        end
      end
      [rows_processed, csv_file_paths, table_schemas]
    end
  end
end
