
require 'etl/transform/base.rb'

module ETL::Transform

  # Removes newlines from string values in row
  class RemoveNewlines < Base
    def initialize()
      super()
    end

    # Truncates the date
    def transform(row)
      new_row = {}
      row.each do |key, v|
        if v.is_a? String
            new_row[key] = v.gsub("\n", " ") if v.is_a? String
        else
          new_row[key] = row[key]
        end
      end
      new_row
    end
  end
end
