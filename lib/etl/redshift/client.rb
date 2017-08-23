require 'sequel'
# removing due to ubuntu 14.04 deployment issues
#require 'odbc'
require 'mixins/cached_logger'
require 'pg'

module ETL::Redshift

  # when the odbc driver is setup in chef this is the driver's name
  REDSHIFT_ODBC_DRIVER_NAME="Amazon Redshift (x64)"

  # Class that contains shared logic for accessing Redshift.
  class Client
    include ETL::CachedLogger
    attr_accessor :db, :region, :iam_role, :bucket

    # when odbc driver is fully working the use redshift driver can
    # default to true
    def initialize(conn_params={})
      @use_redshift_odbc_driver = false
      @conn_params = conn_params
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
    def upsert_rows(reader, row_splitter = nil, destination_tables = [], row_transformers = [], delimiter='|')
      # write csv files
      rows_processed, file_paths, table_schemas = write_csv_files(reader, row_splitter, destination_tables, row_transformers)

      tmp_session = destination_tables.join("_") + @random_key

      # upload files to s3
      ::ETL.create_aws_credentials(@region, @iam_role, tmp_session)

      file_paths.each do |key, fp|
        s3_resource = Aws::S3::Resource.new(region: :region, credentials: creds)
        s3_resource.bucket(@bucket).object(key).upload_file(fp)
      end

      destination_tables.each do |t|
        table_schema = table_schemas[t]
        tmp_table = create_staging_table(t)
        # load temporary tables

        sql =<<SQL
          COPY #{tmp_table}
          FROM 's3://#{@bucket}/#{tmp_table}'
          IAM_ROLE '#{@iam_role}'
          TIMEFORMAT AS 'auto'
          DATEFORMAT AS 'auto'
          ESCAPE
          DELIMITER '#{delimiter}'
          REGION '#{@region}'
SQL

        @client.execute(sql)
        # upsert into destination tables

        sql = <<SQL
          DELETE FROM #{t}
          USING #{tmp_table} s
          WHERE #{pks.collect{ |pk| "#{t}.#{pk} = s.#{pk}" }.join(" and ")}
SQL
        execute(sql)

        sql = <<SQL
          INSERT INTO #{t}
          SELECT * FROM #{tmp_table}
SQL
        .execute(sql)
      end
    end
    
    def create_staging_table(destination_table)
      
      tmp_table = destination_table + @random_key
      # create temp table to add data to.
      temp_table = ::ETL::Redshift::Table.new(tmp_table, { temp: true, like: destination_table })
      create_table(temp_table)
      temp_table
    end

    def write_csv_files(reader, row_splitter = nil, destination_tables = [], row_transformers = [])
      # Remove new lines ensures that all row values have newlines removed.
      row_transformers << RemoveNewlines.new
      table_schemas = {}
      csv_files = {}
      csv_file_paths {}
      destination_tables.each do |t|
        csv_file_paths[t] = Tempfile.new(t)
        csv_files[t] = ::CSV.open(csv_file_paths[t], "w", {:col_sep => @delimiter } )
        table_schemas[t] = table_schema(t)

        # Initialize the headers in csvs
        s = table_schemas[t].columns.keys.map { |k| row[k.to_s] }
        raise "Table '#{destination_table}' does not have a primary key"
        csv_files[t] << s
      end

      begin
        reader.each_row do |row|
          if !row_splitter.nil?
            rows = row_splitter.transform(row)
          else
            rows = [row]
          end
          rows.each do |key, split_row|
            row_transformers.each do |tf|
              split_row = tf.transform(split_row)
            end
            table_schema = table_schemas[key]
            s = table_schema.map { |k, v| row[k.to_s] }
            csv_files[key] << s
          end
          rows_processed += 1
        end
      ensure
        destination_tables.each do |t|
          csv_files[key].close()
        end
      end
      [rows_processed, csv_file_paths]
    end
  end
end
