require 'etl'
require 'etl/cache/base'

RSpec.describe "cache" do
  it "properly finds cached row" do
      data = [
        { "id" => "1", "bento" => "b1", "info" => "foo" },
        { "id" => "1", "bento" => "b2", "info" => "foo1" },
        { "id" => "2", "bento" => "b2", "info" => "other" },
      ]
      cache = ::ETL::Cache::Base.new(["id"])
      cache.fill(data)
      expect(cache.find_rows({"id" => 1})).to eq([{"id"=>"1", "bento"=>"b1", "info"=>"foo"}, {"id"=>"1", "bento"=>"b2", "info"=>"foo1"}])
      expect(cache.find_rows({"id" => 2})).to eq([{"id"=>"2", "bento"=>"b2", "info"=>"other"}])
      expect(cache.find_rows({"id" => 3})).to eq(nil)
  end
end
