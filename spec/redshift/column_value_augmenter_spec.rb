require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/redshift/column_value_augmenter'
require 'etl/core'

module Test
  class Augmenter< ::ETL::Transform::Base
    def initialize(augmenter)
      @augmenter = augmenter
      @count = 10
    end
    def transform(row)
      row = @augmenter.transform(row)
      row["dw_id"] = @count.to_s if row["dw_id"].nil?
      @count = @count + 1
      row
    end
  end
  class Selector
    def select_row(current_row, found_rows)
      return nil if found_rows.nil? || found_rows.count == 0
      bento = current_row["bento"]
      current = nil
      found_rows.each do |fr|
        if fr["bento"] == bento
          return fr
        end
        if fr["h_current"] == "t"
          current = fr
        end
      end
      current
    end
  end
end

RSpec.describe "redshift column_augmenter" do
  context "Test redshift column_augmenter" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "test_table_2" }
    it "augment a row with a surrogate key and old values" do
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name)
      table.string(:id)
      table.string(:dw_id)
      table.string(:info)
      table.string(:bento)
      table.date(:h_created_at)
      table.date(:h_ended_at)
      table.boolean(:h_current)
      table.add_primarykey(:dw_id)
      client.create_table(table)
      data = [
        { "id" => "1", "bento" => "a", "info" => "foo" },
        { "id" => "1", "bento" => "b", "info" => "foo" },
        { "id" => "2", "bento" => "c", "info" => "other" },
      ]
      input = ETL::Input::Array.new(data)
      table_schema = client.table_schema(table_name)
      # insert test data
      client.execute("INSERT INTO #{table_name} (dw_id, id, info, bento, h_current) VALUES ('6', '1', 'bar', 'a', true)")

      column_augmenter = ::ETL::Redshift::ColumnValueAugmenter.new(client, table_schema, ["dw_id"], ["id"], ["info", "bento"], nil, Test::Selector.new)
      # Wrapping the column augmenter so that rows that don't have dw_id's set will get values
      augmenter = Test::Augmenter.new(column_augmenter)
      client.upsert_rows(input, {table_schema.name => table_schema}, augmenter)

      r = client.execute("Select * from #{table_name} ORDER BY dw_id")
      values = []
      r.each { |h| values << h }
      expect(values.count).to eq(3)
      expect(values[0]).to eq({"id"=>"2", "dw_id"=>"12", "info"=>"other", "bento"=>"c", "h_created_at"=>nil, "h_ended_at"=>nil, "h_current"=>nil})
      expect(values[1]).to eq({"id"=>"1", "dw_id"=>"6", "info"=>"foo", "bento"=>"a", "h_created_at"=>nil, "h_ended_at"=>nil, "h_current"=>"t"})
      expect(values[2]).to eq({"id"=>"1", "dw_id"=>"6", "info"=>"foo", "bento"=>"b", "h_created_at"=>nil, "h_ended_at"=>nil, "h_current"=>"t"})
      client.drop_table(table_name)
    end
  end
end
