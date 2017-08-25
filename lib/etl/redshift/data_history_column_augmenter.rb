require_relative "../transform/id_augmenter"

module ETL::Redshift
  # Augments the row with values we need to use for
  # processing. Doing these steps in one place as the cache
  # lookup is the same.
  class DataHistoryColumnAugmenter <::ETL::Transform::Base
    def initialize(client, table_schema, surrogate_key, natural_keys, old_columns, filter_part)
      pks = table_schema.primary_key
      raise "Support converting multiple pks to one dw pk on #{table_schema.name}" if pks.count > 1
      raise "No primary found on table #{table_schema.name}" if pks.count == 0

      primary_key = pks[0]
      query = "Select #{primary_key}, #{natural_keys.join(", ")}, #{surrogate_key}, #{old_columns.join(", ")} from #{table_schema.name}"
      query = query + "WHERE #{filter_part}" if !filter_part.nil?

      r = client.execute(query)
      # Adding a restriction to not bring excessively large amounts of data into in memory cache.
      # If this limit is reached likely time to put this in redis instead.
      raise "Add a where to ensure the number of rows in memory is smaller than 1000000 rows" if r.ntuples > 1000000
      @id_augmenter = ::ETL::Transform::IDAugmenter(surrogate_key, natural_keys, r)
      @previous_augmenter = ::ETL::Transform::PreviousColumnAugmenter(old_columns)
    end

    def transform(row)
      row = @id_augmenter.transform(row)
      @previous_augmenter.transform(row)
    end
  end
end
