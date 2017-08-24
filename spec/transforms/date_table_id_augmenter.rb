require 'etl/core'

RSpec.describe "transforms" do
  it "augment date_table id to row" do
    data = [
      { "id" => "1", "ct" => "2015-03-31 01:12:34" },
      { "id" => "2", "ct" => "1999-01-08 04:05:06 -8:00" },
      { "id" => "3", "ct" => DateTime.new(2001,2,3,4,5,6) },
      { "id" => "4", "ct" => Date.new(2001,2,4) },
    ]
    augmenter = ::ETL::Transform::DateTableIDAugmenter.new(["ct"])

    updated_data = []
    data.each do |row|
      updated_data << augmenter.transform(row)
    end

    expect(updated_data.count).to eq(4)
    output = {}
    updated_data.each do |value|
      output[value["id"]] = value["ct_dt_id"]
    end
    expect(output).to eq({"1"=>20150331, "2"=>19990108, "3"=>20010203, "4"=>20010204})
  end
end


