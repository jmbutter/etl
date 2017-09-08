require 'etl/core'
require 'etl/output/redshift2'
require 'etl/transform/row_splitter'

def rspec_aws_params
  ETL.config.aws[:test]
end

def rspec_redshift_params
  ETL.config.redshift[:test]
end

class Helper
  def self.client
    @@client ||= ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test])
  end
end

def table1
  table = ETL::Redshift::Table.new("test1")
  table.string(:id)
  table.string(:bento)
  table.add_primarykey(:id)
  table
end

def table2
  table2 = ETL::Redshift::Table.new("test2")
  table2.string(:id)
  table2.string(:info)
  table2.add_primarykey(:id)
  table2
end

RSpec.describe "redshift2" do
  let(:client) {  ::Helper.client }
  let(:table_name) { table1.name }
  let(:table_name_2) { table2.name }
  context "Test redshift2 out can upsert data" do

    it "Add new rows to tables" do
      ::Helper.client.drop_table(table1.name)
      ::Helper.client.drop_table(table2.name)
      ::Helper.client.create_table(table1)
      ::Helper.client.create_table(table2)
      sleep(5)

      data = [
        { "id" => "4", "info" => "bar", "bento" => "c" },
        { "id" => "1", "info" => "bar", "bento" => "a" },
        { "id" => "5", "info" => "other", "bento" => "a" },
      ]

      # test output to multiple tables.
      input = ETL::Input::Array.new(data)
      table1_schema = client.table_schema(table_name)
      table2_schema = client.table_schema(table_name_2)
      transformer = ::ETL::Transform::SplitRow.SplitByTableSchemas([table1_schema, table2_schema])
      table_schema_lookup = { table_name => table1_schema, table_name_2 => table2_schema }

      output = ::ETL::Output::Redshift2.new(client, table_schema_lookup, transformer)
      output.reader = input
      result = output.run

      r = client.execute("Select * from #{table_name} ORDER BY id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"id"=>"1", "bento"=>"a"}, {"id"=>"4", "bento"=>"c"}, {"id"=>"5", "bento"=>"a"}])

      r = client.execute("Select * from #{table_name_2} ORDER BY id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"id"=>"1", "info"=>"bar"}, {"id"=>"4", "info"=>"bar"}, {"id"=>"5", "info"=>"other"}])

      single_table_data = [
        { "id" => "10", "info" => "bar", "bento" => "c" },
      ]

      # test when there is only one table used.
      output = ::ETL::Output::Redshift2.new(client, { table_name => table1_schema}, nil)
      input = ETL::Input::Array.new(single_table_data)
      output.reader = input
      result = output.run

      r = client.execute("Select * from #{table_name} ORDER BY id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"id"=>"1", "bento"=>"a"}, {"id"=>"10", "bento"=>"c"}, {"id"=>"4", "bento"=>"c"}, {"id"=>"5", "bento"=>"a"}])
    end
  end
end

