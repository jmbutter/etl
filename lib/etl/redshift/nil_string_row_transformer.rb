module ETL
  module Redshift
    # Will convert values that are nil for string to a specified string
    # to support s3 data being copied into redshift as null.
    class NilStringRowTransformer
      def initialize(table_schemas_lookup, null_string_value, symbolized_keys = true)
        @null_string_value = null_string_value
        @string_columns_hash_by_table = {}
        table_schemas_lookup.each_pair do |k, t|
          @string_columns_hash_by_table[k.to_s] = t.string_columns.each
        end
        @symbolized_keys = symbolized_keys
      end

      def transform(row)
        if @string_columns_hash_by_table.keys.count == 1
          transform_single_table_row(row)
        else
          transform_multi_table_row(row)
        end
        row
      end

      private

      def transform_single_table_row(row)
        columns = @string_columns_hash_by_table.values[0]
        columns.each do |key|
          row[key.to_sym] = @null_string_value if @symbolized_keys && row[key.to_sym].nil?
          row[key] = @null_string_value if !@symbolized_keys && row[key].nil?
        end
      end

      def transform_multi_table_row(row)
        @string_columns_hash_by_table.each_key do |table_name|
          next unless row.key?(table_name)
          string_columns = @string_columns_hash_by_table[table_name]
          string_columns.each do |key|
            result = row[table_name]
            if result.is_a?(Array)
              result.each do |row|
                row[key.to_sym] = @null_string_value if @symbolized_keys && row[key.to_sym].nil?
                row[key] = @null_string_value if !@symbolized_keys && row[key].nil?
              end
            else
              result[key.to_sym] = @null_string_value if @symbolized_keys && result[key.to_sym].nil?
              result[key] = @null_string_value if !@symbolized_keys && result[key].nil?
            end
          end
        end
      end
    end
  end
end
