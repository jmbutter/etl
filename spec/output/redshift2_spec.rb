require 'etl/core'
require 'etl/output/redshift2'

def rspec_aws_params
  ETL.config.aws[:test]
end

def rspec_redshift_params
  ETL.config.redshift[:test]
end
class TestTransformer
  def transform(row)
    row["added"] = "Hit"
    row
  end
end

class IncrementingTestIDGenerator
  def initialize(start=0)
    @count = start
  end
  def generate_id
    @count = @count  + 1
    @count.to_s
  end
end

class RowPipelineHook
  def select_row(current_row, found_rows)
    return nil if found_rows.nil?
    bento = current_row["bento"]
    row_marked_current = nil
    found_rows.each do |r|
      if r["h_current"] == "t"
        row_marked_current = r
      end
      if r["bento"] == bento
        return r
      end
    end
    row_marked_current
  end

  def pre_update?
    true
  end

  def pre(row)
    row
  end

  def post_update?
    true
  end
  def post(row)
    row
  end

  def pre_split_process?
    true
  end
  def pre_split_process(named_rows)
    named_rows
  end
  def post_split_process?
    true
  end
  def post_split_process(named_rows)
    named_rows
  end

  def skip?(row)
    return true if row["id"] == "zzzz"
    false
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
    DateTime.new(@year,@month,@day,4,5,6)
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
  table.string(:added)
  table.add_primarykey(:dw_id)
  table
end

def orgs_history_table
  table2 = ETL::Redshift::Table.new("test_orgs_history")
  table2.string(:h_id)
  table2.string(:dw_id)
  table2.string(:id)
  table2.string(:bento)
  table2.date(:h_created_at)
  table2.date(:h_ended_at)
  table2.boolean(:h_current)
  table2.add_primarykey(:h_id)
  table2
end

RSpec.describe "redshift2" do
  before(:example) do
    ::Helper.client.drop_table(orgs_table.name)
    ::Helper.client.drop_table(orgs_history_table.name)
    ::Helper.client.create_table(orgs_table)
    ::Helper.client.create_table(orgs_history_table)
    sleep(5)
  end
  let(:client) {  ::Helper.client }
  let(:table_name) { orgs_table.name }
  let(:table_name_2) { orgs_history_table.name }
  let(:date) { DateTime.new(2001,5,10,4,5,6) }
  context "Test redshift2 can upsert data to a data and history table multiple table" do

    it "Add new row and existing one row with with bento non-existing history records in " do
      # Adding data so that when data comes in it will find this pre-existing id
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, h_created_at, bento, h_current) VALUES ('1','3','4', '2017.5.22', 'a', true )")
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, h_created_at, h_ended_at, bento, h_current) VALUES ('2','3','4', '2017.5.22', '2017.5.23', 'b', false )")
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, h_created_at, bento, h_current) VALUES ('3','4','5', '2017.5.22', 'a', true )")
      data = [
        { "id" => "4", "info" => "bar", "bento" => "c" },
        { "id" => "1", "info" => "bar", "bento" => "a" },
        { "id" => "5", "info" => "other", "bento" => "a" },
        { "id" => "zzzz", "info" => "other", "bento" => "a" },
      ]
      input = ETL::Input::Array.new(data)

      output = ::ETL::Output::Redshift2.new(client, table_name, table_name_2, "dw_id", ["id"], ["bento"])
      output.id_generator = IncrementingTestIDGenerator.new(5)
      output.now_generator = TestCurrentDateTimeGenerator.new
      output.reader = input
      output.row_pipeline_hook = RowPipelineHook.new
      output.pre_transformer = TestTransformer.new
      result = output.run

      r = client.execute("Select * from #{table_name} ORDER BY dw_id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"dw_id"=>"3", "info"=>"bar", "added"=>"Hit"},
                            {"dw_id"=>"4", "info"=>"other", "added"=>"Hit"},
                            {"dw_id"=>"8", "info"=>"bar", "added"=>"Hit"}])

      r = client.execute("Select * from #{table_name_2} ORDER BY h_id")
      values = []
      r.each { |h| values << h }
      expect(values).to eq([{"h_id"=>"1", "dw_id"=>"3", "id"=>"4", "bento"=>"a", "h_created_at"=>"2017-05-22", "h_ended_at"=>"2001-05-10", "h_current"=>"f"},
                            {"h_id"=>"2", "dw_id"=>"3", "id"=>"4", "bento"=>"b", "h_created_at"=>"2017-05-22", "h_ended_at"=>"2017-05-23", "h_current"=>"f"},
                            {"h_id"=>"3", "dw_id"=>"4", "id"=>"5", "bento"=>"a", "h_created_at"=>"2017-05-22", "h_ended_at"=>nil, "h_current"=>"t"},
                            {"h_id"=>"6", "dw_id"=>"3", "id"=>"4", "bento"=>"c", "h_created_at"=>"2001-05-10", "h_ended_at"=>nil, "h_current"=>"t"},
                            {"h_id"=>"7", "dw_id"=>"8", "id"=>"1", "bento"=>"a", "h_created_at"=>"2001-05-10", "h_ended_at"=>nil, "h_current"=>"t"}])
    end
  end

  context "Test DataHistoryRowTransformer" do
    it "New data row should generate new history row" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      date_time_gen = TestCurrentDateTimeGenerator.new
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new, orgs, orgs_history, date_time_gen, RowPipelineHook.new)
      created_row = t.transform({"id" => "4", "bento" => "app1a", "info" => "info1"})
      expect(created_row).to eq({
        "test_orgs"=>{"info"=>"info1", "dw_id"=>"2" },
        "test_orgs_history"=>{"id"=>"4", "bento"=>"app1a", "h_id"=>"1", "dw_id"=>"2", "h_current"=>true, "h_created_at" => date }})
    end

    it "Non slowly changing dimension change should only have a change to the data table" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      date_time_gen = TestCurrentDateTimeGenerator.new
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, bento, h_created_at, h_current) VALUES ('11', '13', '18', 'app1a', '2001-05-10T04:05:06+00:00', true)")
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new, orgs, orgs_history, date_time_gen, RowPipelineHook.new)
      created_row = t.transform({"id" => "18", "bento" => "app1a", "info" => "info1"})
      expect(created_row).to eq({"test_orgs"=>{"dw_id"=>"13", "info"=>"info1"}})
    end

    it "A Change to the slowly changing dimension should produce a new row for history and update the existing one" do
      orgs = client.table_schema(table_name)
      orgs_history = client.table_schema(table_name_2)
      client.execute("INSERT INTO #{table_name_2} (h_id, dw_id, id, bento, h_created_at, h_current) VALUES ('10', '12', '5', 'app1a', '2001-05-10T04:05:06+00:00', true)")
      date_time_gen = TestCurrentDateTimeGenerator.new
      t = ::ETL::Output::DataHistoryRowTransformer.new(client, ["bento"], ["id"], IncrementingTestIDGenerator.new(10), orgs, orgs_history, date_time_gen, RowPipelineHook.new)
      created_row = t.transform({"id" => "5", "bento" => "app1b"})
      expect(created_row).to eq({
        "test_orgs"=>{"dw_id"=>"12"},
        "test_orgs_history"=>[
          {"h_id"=>"10", "dw_id"=>"12", "id"=>"5", "bento"=>"app1a", "h_created_at" => "2001-05-10", "h_ended_at" => date, "h_current"=>false },
          {"h_id"=>"11", "dw_id"=>"12", "id"=>"5", "bento"=>"app1b", "h_created_at" => date, "h_ended_at" => nil, "h_current"=>true}
        ]})
    end
  end
end

