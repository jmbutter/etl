require "base64"
require "securerandom"

module ETL::Transform
  # Matches the natural keys to a surrogate key
  class SurrogateIDAugmenter < Base
    def initialize(surrogate_key, optional_primary_key, natural_keys, cache)
      @natural_keys = natural_keys
      @surrogate_key = surrogate_key
      @primary_key = optional_primary_key
      @cache = cache
    end

    def transform(row)
      found_row = @cache.find_row(row)
      if !found_row.nil?
        puts found_row.inspect
        row[@surrogate_key] = found_row[@surrogate_key] if found_row.has_key?(@surrogate_key)
        row[@primary_key] = found_row[@primary_key] if !@primary_key.nil?
      end
      row
    end
  end
end

