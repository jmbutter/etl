module ETL::Transform

  # splits the row into multiple named rows
  class SplitRow < Base

    # Helper method to enable easy creation of a row
    # splitter based on redshift tables.
    def self.SplitByTableSchemas(table_schemas = [], row_columns_symbolized = true)
      column_hash_maps = {}
      table_schemas.each do |t|
        column_hash_maps[t.name] = t.columns.keys
      end
      ::ETL::Transform::SplitRow.new(column_hash_maps, row_columns_symbolized)
    end

    # column hash map is a hash of arrays of columns.
    def initialize(column_hash_maps={}, row_columns_symbolized)
      super()
      @column_hash_maps = column_hash_maps
      @row_columns_symbolized = row_columns_symbolized
    end

    def transform(row)
      named_rows = {}
      @column_hash_maps.each do |row_name, column_names|
        row_value = {}
        column_names.each do |c|
          if @row_columns_symbolized
            row_value[c.to_sym] = row[c.to_sym] if row.key?(c.to_sym)
          else
            row_value[c] = row[c] if row.key?(c)
          end
        end
        named_rows[row_name] = row_value
      end
      named_rows
    end
  end
end
