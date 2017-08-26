require "base64"
require "securerandom"
require_relative "../cache/base"

module ETL::Transform

  class PreviousColumnValueAugmenter < Base
    def initialize(columns, cache)
      raise "columns cannot be nil" if columns.nil?
      raise "cache cannot be nil" if cache.nil?

      @columns = columns
      @cache = cache
    end

    def transform(new_row)
      old_row = @cache.find_row(new_row)
      if !old_row.nil?
        @columns.each do |c|
          old_value = old_row[c]
          if !old_value.nil?
            new_row["old_#{c}"] = old_value
          end
        end
      end
      new_row
    end
  end
end


