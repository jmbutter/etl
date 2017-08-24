require "base64"
require "securerandom"

module ETL::Transform

  # used for testing only
  class IncrementingTestIDGenerator
    def initialize(start=0)
      @count = start
    end
    def generate_id
      @count = @count  + 1
    end
  end

  class IDUUIDGenerator
    def generate_id
      SecureRandom.uuid
    end
  end

  class IDAugmenter < Base
    def initialize(surrogate_key, natural_keys, reader, id_generator)
      @natural_keys = natural_keys
      @surrogate_key = surrogate_key
      @id_generator = id_generator
      @id_lookup = {}
      reader.each do |row|
        key = ::ETL::Transform::IDAugmenter.hash_natural_keys(@natural_keys, row)
        @id_lookup[key] = row[@surrogate_key]
      end
    end

    def self.hash_natural_keys(columns, row)
      key_values = []
      columns.each do |c|
        key_values << row[c]
      end
      hashed_value = Base64.encode64(key_values.join('|'))
    end

    def transform(row)
      hashed_key = self.class.hash_natural_keys(@natural_keys, row)
      found_key = @id_lookup[hashed_key] if @id_lookup.has_key?(hashed_key)
      found_key = @id_generator.generate_id if found_key.nil?
      row[@surrogate_key] = found_key
      row
    end
  end
end

