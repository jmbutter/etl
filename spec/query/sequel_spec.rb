require 'etl/core'

RSpec.describe "sequel query" do

  let(:select) { ["foo"] }
  let(:from) { "bar" }
  let(:where) { [] }
  let(:group_by) { [] }
  let(:limit) { nil }
  let(:offset) { nil }

  it "select - not array" do
    expect { ETL::Query::Sequel.new("foo", from, where, group_by, limit) }.to raise_error("Select is not array")
  end

  it "select - array of one string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "select - array of several strings" do
  	sequel_query = ETL::Query::Sequel.new(["foo", "oof"], from, where, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT foo, oof FROM #{from}")
  end

  it "select - array of an empty string" do
    expect { ETL::Query::Sequel.new([""], from, where, group_by, limit) }.to raise_error("Select is empty")
  end

  it "select - array of several empty strings" do
    expect { ETL::Query::Sequel.new(["", "", ""], from, where, group_by, limit) }.to raise_error("Select is empty")
  end

  it "from - not string" do
    expect { ETL::Query::Sequel.new(select, select, where, group_by, limit) }.to raise_error("From is not string")
  end

  it "from - empty string" do
    expect { ETL::Query::Sequel.new(select, "", where, group_by, limit) }.to raise_error("From is empty")
  end

  it "where - nil" do
  	sequel_query = ETL::Query::Sequel.new(select, from, nil, group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "where - not array" do
    expect { ETL::Query::Sequel.new(select, from, "", group_by, limit) }.to raise_error("Where is not array")
  end

  it "where - array of several strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["whe = whe", "re < er"], group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE whe = whe AND re < er")
  end

  it "where - array of an empty string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, [""], group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "where - array of several empty strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["", "", ""], group_by, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - nil" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, nil, limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - not array" do
    expect { ETL::Query::Sequel.new(select, from, where, "", limit) }.to raise_error("Group_by is not array")
  end

  it "group_by - array of several strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, ["group", "by"], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} GROUP BY group, by")
  end

  it "group_by - array of an empty string" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, [""], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "group_by - array of several empty strings" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, ["", "", ""], limit)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from}")
  end

  it "limit - int" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 1)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 1")
  end

  it "limit - not int" do
  	expect { ETL::Query::Sequel.new(select, from, where, group_by, "one") }.to raise_error("Limit is not Integer")
  end

  it "offset - int without limit" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	sequel_query.set_offset(os)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT #{os} OFFSET #{os}")
  end

  it "offset - int with limit" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 20)
  	sequel_query.set_offset(os)
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 20 OFFSET #{os}")
  end

  it "offset - cancel" do
  	os = 10
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, 20)
  	sequel_query.set_offset(os)
  	sequel_query.cancel_offset
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} LIMIT 20")
  end

  it "offset - not int" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.set_offset("ten") }.to raise_error("Parameter is not Integer")
  end

  it "append_where - array of a string without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	sequel_query.append_where(["whe = whe"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE whe = whe")
  end

  it "append_where - array of several strings without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	sequel_query.append_where(["whe = whe", "re < er"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE whe = whe AND re < er")
  end

  it "append_where - array of a string with where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["where is where"], group_by, limit)
  	sequel_query.append_where(["whe = whe"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND whe = whe")
  end

  it "append_where - array of several strings with where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["where is where"], group_by, limit)
  	sequel_query.append_where(["whe = whe", "re < er"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND whe = whe AND re < er")
  end

  it "append_where - not array" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.append_where("ten") }.to raise_error("Parameter is not Array")
  end

  it "append_replacable_where - array of a string without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)

  	sequel_query.append_replacable_where(["whe = whe"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE whe = whe")

  	sequel_query.append_replacable_where(["next = next"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE next = next")
  end

  it "append_replacable_where - array of several strings without where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)

  	sequel_query.append_replacable_where(["whe = whe", "re < er"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE whe = whe AND re < er")

  	sequel_query.append_replacable_where(["nextwhe = nextwhe", "nextre < nexter"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE nextwhe = nextwhe AND nextre < nexter")
  end

  it "append_replacable_where - array of a string with where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["where is where"], group_by, limit)

  	sequel_query.append_replacable_where(["whe = whe"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND whe = whe")

  	sequel_query.append_replacable_where(["next = next"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND next = next")
  end

  it "append_replacable_where - array of several strings with where" do
  	sequel_query = ETL::Query::Sequel.new(select, from, ["where is where"], group_by, limit)

  	sequel_query.append_replacable_where(["whe = whe", "re < er"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND whe = whe AND re < er")

  	sequel_query.append_replacable_where(["nextwhe = nextwhe", "nextre < nexter"])
  	expect( sequel_query.query ).to eq("SELECT #{select[0]} FROM #{from} WHERE where is where AND nextwhe = nextwhe AND nextre < nexter")
  end

  it "append_replacable_where - not array" do
  	sequel_query = ETL::Query::Sequel.new(select, from, where, group_by, limit)
  	expect { sequel_query.append_replacable_where("ten") }.to raise_error("Parameter is not Array")
  end
end
