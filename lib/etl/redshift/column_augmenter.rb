require_relative "../transform/surrogate_id_augmenter"
require_relative "../transform/previous_column_value_augmenter"
require_relative "../cache/base"

module ETL::Redshift
  # Augments the row with values we need to use for
  # processing for the history table. Doing these steps augmentation
  # steps in once place as its using the same cache
  class ColumnAugmenter <::ETL::Transform::Base
    def initialize(client, table_schema, surrogate_key, natural_keys, tracking_columns, filter_part)
      raise "client cannot be nil" if client.nil?
      raise "table_schema cannot be nil" if table_schema.nil?
      raise "surrogate key cannot be nil" if surrogate_key.nil?
      raise "natural keys cannot be nil" if natural_keys.nil?

      pks = table_schema.primary_key
      raise "Support converting multiple pks to one dw pk on #{table_schema.name}" if pks.count > 1
      raise "No primary found on table #{table_schema.name}" if pks.count == 0

      primary_key = pks[0]
      query = "SELECT #{primary_key}"
      query = query + ", #{natural_keys.join(", ")}" if !natural_keys.nil?
      query = query + ", #{tracking_columns.join(", ")}" if !tracking_columns.nil?
      query = query + ", #{surrogate_key}" if !surrogate_key.nil?
      query = query + " from #{table_schema.name}"
      query = query + "WHERE #{filter_part}" if !filter_part.nil?
      puts query
      r = client.execute(query)
      cache = ::ETL::Cache::Base.new(natural_keys)
      cache.fill(r)

      # Adding a restriction to not bring excessively large amounts of data into in memory cache.
      # If this limit is reached likely time to put this in redis instead.
      raise "Add a where to ensure the number of rows in memory is smaller than 1000000 rows" if r.ntuples > 1000000
      @previous_augmenter = ::ETL::Transform::PreviousColumnValueAugmenter.new(tracking_columns, cache)
      @id_augmenter = ::ETL::Transform::SurrogateIDAugmenter.new(surrogate_key, primary_key, natural_keys, cache)
    end

    def transform(row)
      row = @id_augmenter.transform(row)
      new_row = @previous_augmenter.transform(row)
      new_row
    end
  end
end
