require_relative "../transform/column_value_augmenter"
require_relative "../cache/base"

module ETL::Redshift
  # Augments the row with values we need to use for
  # processing for the history table. Doing these steps augmentation
  # steps in once place as its using the same cache
  class ColumnValueAugmenter <::ETL::Transform::ColumnValueAugmenter
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
      query = query + ", created_at, ended_at"
      query = query + ", #{surrogate_key}" if !surrogate_key.nil?
      query = query + " from #{table_schema.name}"
      query = query + "WHERE #{filter_part}" if !filter_part.nil?
      r = client.execute(query)
      cache = ::ETL::Cache::Base.new(natural_keys)
      cache.fill(r)

      # Adding a restriction to not bring excessively large amounts of data into in memory cache.
      # If this limit is reached likely time to put this in redis instead.
      raise "Add a where to ensure the number of rows in memory is smaller than 1000000 rows" if r.ntuples > 1000000
      augmenting_columns = [surrogate_key, primary_key, "created_at", "ended_at" ]
      natural_keys.each { |k| augmenting_columns << k }
      super(augmenting_columns, tracking_columns, cache)
    end
  end
end
