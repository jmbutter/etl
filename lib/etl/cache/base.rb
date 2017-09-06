module ETL::Cache
  # Simple in-memory cache used primarily for surrogate
  # key lookups.
  class Base
    def self.hash_column_values(columns, row)
      key_values = []
      columns.each do |c|
        key_values << row[c]
      end
      Base64.encode64(key_values.join('|'))
    end

    def initialize(columns)
      raise "columns nil" if columns.nil?
      raise "columns are not enumerable #{columns.inspect}" if !columns.is_a? Enumerable
      @columns = columns
    end

    def fill(reader)
      @row_lookup = {}
      reader.each do |row|
        key = self.class.hash_column_values(@columns, row)
        if @row_lookup.key?(key)
          @row_lookup[key] << row
        else
          @row_lookup[key] = [row]
        end
      end
    end

    def find_rows(row)
        key = self.class.hash_column_values(@columns, row)
        @row_lookup[key]
    end
  end
end
