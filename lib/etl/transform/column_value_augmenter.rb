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

    def select_row(current_row, found_rows)
      return nil if found_rows.nil?
      raise "Expected only one row to find in the set of rows, cannot proceed" if found_rows.count > 1
      found_rows[0]
    end

    def transform(row)
      found_rows = @cache.find_rows(row)
      found_row = select_row(row, found_rows)
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

