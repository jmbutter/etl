require "base64"
require "securerandom"

module ETL::Transform

  class PreviousValueAugmenter < Base
    def initialize(columns, reader, id_lookup)
      @columns = columns
      @id_lookup = id_lookup
      if !reader.nil?
        reader.each do |row|
          key = ::ETL::Transform::IDAugmenter.hash_natural_keys(@natural_keys, row)
          @id_lookup[key] = row
        end
      end
    end

    def transform(new_row)
      hashed_key = ::ETL::Transform::IDAugmenter.hash_natural_keys(@natural_keys, new_row)
      old_row = @id_lookup[hashed_key] if @id_lookup.has_key?(hashed_key)
      if !row.nil?
        @columns.each do |c|
          old_value = old_row[c]
          if !value.nil?
            row["old_${c}"] = old_value
          end
        end
      end
      row
    end
  end
end


