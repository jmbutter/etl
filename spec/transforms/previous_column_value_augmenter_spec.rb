require 'etl/core'

RSpec.describe "transforms" do
  it "Add previous old values to row" do
    reader = [
      { "id" => "1", "i" => "b" },
      { "id" => "2", "i" => "f" },
    ]
    cache = ::ETL::Cache::Base.new(["id"])
    cache.fill(reader)
    augmenter = ::ETL::Transform::PreviousColumnValueAugmenter.new(["i"], ["id"], cache)

    input = [
      { "id" => "1", "i" => "c" },
      { "id" => "2", "i" => "f" },
    ]
    new_rows = []
    input.each do |row|
      new_rows << augmenter.transform(row)
    end
    expect(new_rows).to eq([{"id"=>"1", "i"=>"c", "old_i"=>"b"}, {"id"=>"2", "i"=>"f", "old_i"=>"f"}])
  end
end



