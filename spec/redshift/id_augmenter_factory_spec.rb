require 'etl/redshift/client'
require 'etl/redshift/table'
require 'etl/redshift/id_augmenter_factory'
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
      lookup_data = [
        { "id" => "1", "dw_id" => "4" },
        { "id" => "2", "dw_id" => "5" },
      ]
      input = ETL::Input::Array.new(lookup_data)
      client.upsert_rows(input, [table_name])

      f = ::ETL::Redshift::IDAugmenterFactory.new(client, table_name, ["id"])
      id_augmenter = f.create_augmenter()

      data_to_augment = [
        { "id" => "1" },
        { "id" => "2" },
      ]
      augment_data_input = ETL::Input::Array.new(data_to_augment)

      updated_data = []
      augment_data_input.each_row do |row|
        updated_data << id_augmenter.transform(row)
      end

      expect(updated_data.count).to eq(2)
      expect(updated_data).to eq([{"id"=>"1", "dw_id"=>"4"},
                                  {"id"=>"2", "dw_id"=>"5"}])
      client.drop_table(table_name)
    end
  end
end
