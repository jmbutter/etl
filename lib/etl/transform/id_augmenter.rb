require "base64"
require "securerandom"

module ETL::Transform

  class IDUUIDGenerator
    def generate_id
      SecureRandom.uuid
    end
  end

  class IDAugmenterFactory
    def initialize(surrogate_key, natural_keys, reader, id_generator=ETL::Transform::IDUUIDGenerator.new)
      @natural_keys = natural_keys
      @surrogate_key = surrogate_key
      @reader = reader
      @id_generator = id_generator
    end

    def create_augmenter()
      id_lookup = {}
      @reader.each do |row|
        key = ::ETL::Transform::IDAugmenter.hash_natural_keys(@natural_keys, row)
        id_lookup[key] = row[@surrogate_key]
      end
      ::ETL::Transform::IDAugmenter.new(@surrogate_key, @natural_keys, id_lookup, @id_generator)
    end
  end

  class IDAugmenter < Base
    def initialize(surrogate_key, natural_keys, id_lookup, id_generator)
      @natural_keys = natural_keys
      @id_lookup = id_lookup
      @surrogate_key = surrogate_key
      @id_generator = id_generator
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

