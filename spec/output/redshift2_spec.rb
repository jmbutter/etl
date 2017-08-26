require 'etl/core'
require 'etl/output/redshift2'

def rspec_aws_params
  ETL.config.aws[:test]
end

def rspec_redshift_params
  ETL.config.redshift[:test]
end

class IncrementingTestIDGenerator
  def initialize(start=0)
    @count = start
  end
  def generate_id
    @count = @count  + 1
  end
end

class TestCurrentDateTimeGenerator
  attr_accessor :year, :month, :day

  def initialize
    @year = 2001
    @month = 5
    @day = 10
  end
  def now
    DateTime.new(@year,@month,@day,4,5,6).iso8601
  end
end

class Helper
  def self.client
    @@client ||= ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test])
  end
end

def orgs_table
  table = ETL::Redshift::Table.new("test_orgs")
  table.string(:dw_id)
  table.string(:info)
  table.add_primarykey(:dw_id)
  table
end

def orgs_history_table
  table2 = ETL::Redshift::Table.new("test_orgs_history")
  table2.string(:h_id)
  table2.string(:dw_id)
  table2.string(:id)
  table2.string(:bento)
  table2.date(:start_date)
  table2.date(:end_date)
  table2.boolean(:current)
  table2.add_primarykey(:h_id)
  table2
end

RSpec.describe "redshift2" do
  before :all do
    ::Helper.client.drop_table(orgs_table.name)
    ::Helper.client.drop_table(orgs_history_table.name)
    ::Helper.client.create_table(orgs_table)
    ::Helper.client.create_table(orgs_history_table)
  end
  let(:client) {  ::Helper.client }
  let(:table_name) { orgs_table.name }
  let(:table_name_2) { orgs_history_table.name }
  context "Test redshift2 can upsert data to a data and history table multiple table" do
    it "augment a row with surrogate key" do

      # Adding data so that when data comes in it will find this pre-existing id
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, start_date, current) VALUES ('1','3','4', '2017-5-22', true )")
      data = [
        { "id" => "4", "info" => "bar", "bento" => "a" },
        { "id" => "5", "info" => "foo", "bento" => "b" },
      ]
      input = ETL::Input::Array.new(data)

      output = ::ETL::Output::Redshift2.new(client, table_name, table_name_2, "dw_id", ["id"], ["bento"])
      output.id_generator = IncrementingTestIDGenerator.new(5)
      output.now_generator = TestCurrentDateTimeGenerator.new
      output.reader = input
      result = output.run

      r = client.execute("Select * from #{table_name_2} ORDER BY dw_id")
      values = []
      r.each { |h| values << h }
      expect(r.values).to eq([])
    end
  end
  
  context "Test DataHistoryRowTransformer" do
    it "New data row should generate new history row" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      date_time_gen = TestCurrentDateTimeGenerator.new
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new, orgs, orgs_history, date_time_gen)
      created_row = t.transform({"id" => "4", "bento" => "app1a", "info" => "info1"})
      expect(created_row).to eq({"test_orgs"=>{"info"=>"info1", "dw_id"=>2 }, "test_orgs_history"=>{"id"=>"4", "bento"=>"app1a", "h_id"=>1, "dw_id"=>2, "current"=>true, "created_at"=>"2001-05-10T04:05:06+00:00"}})
    end

    it "Non slowly changing dimension change should only have a change to the data table" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      date_time_gen = TestCurrentDateTimeGenerator.new
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, bento, start_date, current) VALUES ('11', '13', '18', 'app1a', '2001-05-10T04:05:06+00:00', true)")
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new, orgs, orgs_history, date_time_gen)
      created_row = t.transform({"id" => "18", "bento" => "app1a", "info" => "info1"})
      expect(created_row).to eq({"test_orgs"=>{"dw_id"=>"13", "info"=>"info1"}})
    end

    it "A Change to the slowly changing dimension should produce a new row for history and update the existing one" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, bento, start_date, current) VALUES ('10', '12', '5', 'app1a', '2001-05-10T04:05:06+00:00', true)")
      date_time_gen = TestCurrentDateTimeGenerator.new
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new(10), orgs, orgs_history, date_time_gen)
      created_row = t.transform({"id" => "5", "bento" => "app1b"})
      expect(created_row).to eq({
        "test_orgs"=>{"dw_id"=>"12"},
        "test_orgs_history"=>[
          {"h_id"=>"10", "dw_id"=>"12", "id"=>"5", "bento"=>"app1b", "current"=>false, "ended_at"=>"2001-05-10T04:05:06+00:00"},
          {"h_id"=>11, "dw_id"=>"12", "id"=>"5", "bento"=>"app1b", "current"=>true, "started_at"=>"2001-05-10T04:05:06+00:00"}
        ]})
    end
  end
end


