require 'etl/core'
require 'etl/cache/base'

RSpec.describe "transforms" do
  it "augment id to row" do
    reader = [
      { "id1" => "1", "id2" => "3", "dw_id" => "4", "z" => "10" },
      { "id1" => "2", "id2" => "2", "dw_id" => "5", "z" => "11" },
      { "id1" => "3", "id2" => "1", "dw_id" => "6", "z" => "12" },
    ]
    cache = ::ETL::Cache::Base.new(["id1", "id2"])
    cache.fill(reader)
    id_augmenter = ::ETL::Transform::ColumnValueAugmenter.new(["dw_id"], ["z"], cache)

    data_to_augment = [
      { "id1" => "1", "id2" => "3", "o" => "f", "z" => "1" },
      { "id1" => "2", "id2" => "2", "o" => "b", "z" => "2" },
      { "id1" => "3", "id2" => "1", "o" => "c", "z" => "3" },
      { "id1" => "4", "id2" => "10", "o" => "d", "z" => "4" },
    ]
    augment_data_input = ETL::Input::Array.new(data_to_augment)

    updated_data = []
    augment_data_input.each_row do |row|
      updated_data << id_augmenter.transform(row)
    end

    expect(updated_data.count).to eq(4)
    expect(updated_data).to eq([
      {"id1"=>"1", "id2"=>"3", "o"=>"f", "z"=>"1", "dw_id"=>"4", "old_z"=>"10"},
      {"id1"=>"2", "id2"=>"2", "o"=>"b", "z"=>"2", "dw_id"=>"5", "old_z"=>"11"},
      {"id1"=>"3", "id2"=>"1", "o"=>"c", "z"=>"3", "dw_id"=>"6", "old_z"=>"12"},
      {"id1"=>"4", "id2"=>"10", "o"=>"d", "z"=>"4"}])
  end
end

