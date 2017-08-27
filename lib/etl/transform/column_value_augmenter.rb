require "base64"
require "securerandom"

module ETL::Transform
  class ColumnValueAugmenter < Base
    def initialize(augmenting_columns, previous_columns, cache)
      raise "augmenting columns cannot be nil" if augmenting_columns.nil?
      raise "previous columns cannot be nil" if previous_columns.nil?
      raise "cache cannot be nil" if cache.nil?

      @augmenting_columns = augmenting_columns
      @previous_columns = previous_columns
      @cache = cache
    end

    def transform(row)
      found_row = @cache.find_row(row)
      if !found_row.nil?
        @augmenting_columns.each do |c|
          if !row.has_key?(c)
            # Add values that don't exist
            row[c] = found_row[c]
          end
        end
        @previous_columns.each do |c|
          # add values that already exists in "old_column_name"
          # these can be used for other transforms later.
          row["old_#{c}"] = found_row[c]
        end
      end
      row
    end
  end
end

