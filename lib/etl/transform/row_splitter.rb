module ETL::Transform

  # splits the row into multiple named rows
  class SplitRow < Base

    # Helper method to enable easy creation of a row
    # splitter based on redshift tables.
    def self.SplitByTableSchemas(table_schemas = [])
      column_hash_maps = {}
      table_schemas.each do |t|
        column_hash_maps[t.name] = t.columns.keys
      end
      ::ETL::Transform::SplitRow.new(column_hash_maps)
    end

    # column hash map is a hash of arrays of columns.
    def initialize(column_hash_maps={})
      super()
      @column_hash_maps = column_hash_maps
    end

    def transform(row)
      named_rows = {}
      @column_hash_maps.each do |row_name, column_names|
        row_value = {}
        column_names.each do |c|
          if row.key?(c)
            row_value[c] = row[c]
          end
        end
        named_rows[row_name] = row_value
      end
      named_rows
    end
  end
end
