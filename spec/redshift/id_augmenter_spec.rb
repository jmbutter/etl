require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/redshift/id_augmenter'
require 'etl/core'

RSpec.describe "redshift id_augmenter_factory" do
  context "Test redshift augmenter" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "test_table_2" }
    it "augment a row with surrogate key" do
      client.drop_table(table_name)
      table = ETL::Redshift::Table.new(table_name)
      table.string(:id)
      table.string(:dw_id)
      table.add_primarykey(:dw_id)
      client.create_table(table)
      data = [
        { "id" => "1" },
        { "id" => "2" },
      ]
      input = ETL::Input::Array.new(data)
      table_schema = client.table_schema(table_name)
      id_augmenter = ::ETL::Redshift::IDAugmenter.new(client, table_schema, ["id"], nil, ::ETL::Transform::IncrementingTestIDGenerator.new(5))
      client.upsert_rows(input, [table_name], nil, [id_augmenter], '|')

      r = client.execute("Select * from #{table_name} ORDER BY dw_id")
      expect(r.ntuples).to eq(2)
      expect(r.values).to eq([["1", "6"], ["2", "7"]])
      client.drop_table(table_name)
    end
  end
end
