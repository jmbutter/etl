require 'etl/core'

def rspec_aws_params
  ETL.config.aws[:test]
end

def rspec_redshift_params
  ETL.config.redshift[:test]
end

RSpec.describe "redshift2" do
  context "Test redshift2 can upsert data to multiple tables from 1 input" do
    let(:client) { ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL::config.aws[:test]) }
    let(:table_name) { "redshift2_orgs" }
    let(:table_name_2) { "redshift2_orgs_history" }
    it "augment a row with surrogate key" do
      client.drop_table(table_name)
      client.drop_table(table_name_2)

      # Add the tables
      table = ETL::Redshift::Table.new(table_name)
      table.int(:h_id)
      table.string(:dw_id)
      table.set_identity(:h_id)
      client.create_table(table)

      table2 = ETL::Redshift::Table.new(table_name_2)
      table2.string(:dw_id)
      table2.string(:id)
      table2.add_primarykey(:dw_id)
      client.create_table(table2)

      # Adding data so that when data comes in it will find this pre-existing id
      client.execute("INSERT INTO #{table_name_2} (dw_id, id) VALUES ('3','4')")
      data = [
        { "id" => "4" },
        { "id" => "2" },
      ]
      input = ETL::Input::Array.new(data)

      output = ::ETL::Output::Redshift2.new(client, [table_name, table_name_2], "dw_id", ["id"], '|', ::ETL::Transform::IncrementingTestIDGenerator.new(5))
      output.reader = input
      result = output.run
      expect(result.rows_processed).to eq(2)

      r = client.execute("Select * from #{table_name_2} ORDER BY dw_id")
      expect(r.ntuples).to eq(2)
      expect(r.values).to eq([["3", "4"], ["6", "2"]])
    end
  end
end


