require 'etl/transform/base.rb'

module ETL
  module Transform
    class MultiTransformer
      def initialize(transformers = [])
        @transformers = transformers
      end

      def transform(row)
        @transformers.each do |t|
          row = t.transform(row)
        end
        row
      end
    end
  end
end


