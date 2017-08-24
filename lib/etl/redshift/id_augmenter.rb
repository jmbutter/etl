require_relative "../transform/id_augmenter"

module ETL::Redshift
  class IDAugmenter <::ETL::Transform::IDAugmenter
    def initialize(client, table_name, natural_keys, filter_part = nil, id_generator = nil)
      table_schema = client.table_schema(table_name)
      pks = table_schema.primary_key
      raise "Support converting multiple pks to one dw pk on #{table_name}" if pks.count > 1
      raise "No primary key found on table #{table_name}" if pks.count == 0

      surrogate_key = pks[0]
      query = "Select #{natural_keys.join(", ")}, #{surrogate_key} from #{table_name}"
      query = query + "WHERE #{filter_part}" if !filter_part.nil?

      r = client.execute(query)
      # Adding a restriction
      raise "Add a where to ensure the number of rows in memory is smaller than 500000 rows" if r.ntuples > 500000
      super(surrogate_key, natural_keys, r, id_generator)
    end
  end
end
