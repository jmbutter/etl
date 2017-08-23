require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/core'

RSpec.describe "redshift" do
  context "client testing" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "test_table_1" }
    let(:bucket) { ETL.config.aws[:etl][:s3_bucket] }
    let(:random_key) { [*('a'..'z'), *('0'..'9')].sample(10).join }
    let(:s3_destination) { "#{bucket}/#{table_name}_#{random_key}" }
    it "connect and create and delete a table" do
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name, { backup: false, dist_style: 'All'})
      table.string("name")
      table.int("id")
      table.add_primarykey("id")

      client.create_table(table)
      client.drop_table(table_name)
    end

    it "get table schema" do
      client.drop_table(table_name)
      sql = <<SQL
  create table #{table_name} (
    day timestamp,
    day2 timestamptz,
    id integer,
    test varchar(22),
    num numeric(5,2),
    f1 float8,
    f2 float4,
    large_int bigint,
    small_int smallint,
    PRIMARY KEY(id) );
SQL
      client.execute(sql)
      rows = []
      schema = client.table_schema(table_name)

      expect(schema.columns.keys).to eq(["day", "day2", "f1", "f2", "id", "large_int", "num", "small_int", "test"])
      expect(schema.primary_key).to eq(["id"])
    end


    it "get table columns" do
      client.drop_table(table_name)
      sql = <<SQL
  create table #{table_name} (
    day timestamp);
SQL
      client.execute(sql)
      rows = []
      r = client.columns(table_name).each do |r|
        rows << r
      end
      expect(rows.to_s).to eq("[{\"column\"=>\"day\", \"type\"=>\"timestamp without time zone\"}]")
    end

    it "upsert data into one table" do
      client.drop_table("simple_orgs")
      create_table = <<SQL
  create table simple_orgs (
    id integer,
    col2 varchar(20),
    PRIMARY KEY(id) );
SQL
      client.execute(create_table)
      data = [
        { "col1" => 1, "col2" => "value2a" },
        { "col1" => 2, "col2" => "value2b" },
        { "col1" => 3, "col2" => "value2c" },
      ]
      input = ETL::Input::Array.new(data)
      client.upsert_rows(input, ["simple_orgs"])
      r = client.execute("Select * from simple_orgs")
      expect(r.cmd_tuples).to eq(3)
    end


    it "move data by unloading and copying" do
      target_table = "test_target_table_1"
      client.drop_table(table_name)
      sql = "create table #{table_name} (day datetime NOT NULL, attribute varchar(100), PRIMARY KEY (day));"
      client.execute(sql)

      insert_sql = <<SQL
    insert into #{table_name} values
      ('2015-04-01', 'rain'),
      ('2015-04-02', 'snow'),
      ('2015-04-03', 'sun')
SQL
      client.execute(insert_sql)

      client.drop_table(target_table)
      sql = "create table #{target_table} (day datetime NOT NULL, attribute varchar(100), PRIMARY KEY (day));"
      client.execute(sql)

      client.region = ETL.config.aws[:etl][:region]
      client.iam_role = ETL.config.aws[:etl][:role_arn]

      client.unload_to_s3("select * from #{table_name}", s3_destination)
      client.copy_from_s3(target_table, s3_destination)
      expect(client.count_row_by_s3(s3_destination)).to eq(3)

      sql = "select count(*) from #{target_table}"
      result = client.execute(sql)

      expect(result.first["count"].to_i).to eq(3)

      # Delete s3 files
      client.delete_object_from_s3(bucket, table_name, table_name)
    end
  end
end
