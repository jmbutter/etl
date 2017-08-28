
module ETL::Schema

  # Class representing a single column including width and precision
  class Column
    attr_accessor :type, :width, :precision, :nullable, :ordinal_pos, :fk

    def initialize(type, width = nil, precision = nil, nullable=true)
      @type = type.to_sym()
      @width = width
      @precision = precision
      @nullable = nullable
    end

    def set_fk(col, table)
      @fk = {:column => col, :table => table}
    end

    def to_s
      s = type.to_s()
      if not width.nil? or not precision.nil?
        s += "("
        s += width.nil? ? "0" : width.to_s()
        if not precision.nil?
          s += ", #{precision}"
        end
        s += ")"
      end
      return s
    end
  end
end
