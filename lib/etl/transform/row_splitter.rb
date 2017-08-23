module ETL::Transform

  # splits the row into multiple named rows
  class SplitRow < Base

    # column hash map is a hash of arrays of columns.
    def initialize(column_hash_maps={})
      super()
      @column_hash_map = column_hash_map
      @values = {}
      values.to_a.each do |v|
        @values[v] = 1
      end
    end

    def transform(value)
      named_rows = {}
      column_hash_map.each do |row_name, column_names|
        row_value = {}
        column_names.each do |c|
          if value.key?(c)
            row_value[c] = value[c]
          end
        end
        named_rows[row_name] = row_value
      end
      named_rows
    end
  end
end
