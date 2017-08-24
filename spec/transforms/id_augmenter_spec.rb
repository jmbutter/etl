require 'etl/core'

RSpec.describe "transforms" do
  it "augment id to row" do
    reader = [
      { "id1" => "1", "id2" => "3", "dw_id" => "4" },
      { "id1" => "2", "id2" => "2", "dw_id" => "5" },
      { "id1" => "3", "id2" => "1", "dw_id" => "6" },
    ]
    id_augmenter = ::ETL::Transform::IDAugmenter.new("dw_id", ["id1", "id2"], reader, ::ETL::Transform::IncrementingTestIDGenerator.new(14))

    data_to_augment = [
      { "id1" => "1", "id2" => "3" },
      { "id1" => "2", "id2" => "2" },
      { "id1" => "3", "id2" => "1" },
      { "id1" => "4", "id2" => "10" },
    ]
    augment_data_input = ETL::Input::Array.new(data_to_augment)

    updated_data = []
    augment_data_input.each_row do |row|
      updated_data << id_augmenter.transform(row)
    end

    expect(updated_data.count).to eq(4)
    expect(updated_data).to eq([{"id1"=>"1", "id2"=>"3", "dw_id"=>"4"},
                                {"id1"=>"2", "id2"=>"2", "dw_id"=>"5"},
                                {"id1"=>"3", "id2"=>"1", "dw_id"=>"6"},
                                {"id1"=>"4", "id2"=>"10", "dw_id"=>15}])
  end
end

