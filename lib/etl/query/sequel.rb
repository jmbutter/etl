module ETL::Query

  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class Sequel
    attr_accessor :where, :group_by, :limit, :offset

    def initialize(select, from, where = nil, group_by = nil, limit = nil)
      @select = select 
      @from = from 
      @where = where 
      @group_by = group_by 
      @limit = limit 
      @offset = nil
    end

    def query
      where = 
        if @where.nil?
          if defined? @tmp_where && !@tmp_where.nil?
            "WHERE #{@tmp_where}"
          else
            ""
          end
        else
          if defined? @tmp_where && !@tmp_where.nil?
            "WHERE #{@where} AND #{@tmp_where}"
          else
            "WHERE #{@where}"
          end
        end

      group_by = 
        if @group_by.nil?
          ""
        else
          "GROUP BY #{@group_by}"
        end

      limit =
        if @limit.nil?
          ""
        else
          "LIMIT #{@limit}"
        end

      offset =
        if @offset.nil?
          ""
        else
          "OFFSET #{@offset}"
        end

<<-EOS
  	  SELECT #{@select} FROM #{@from} #{where} #{group_by} #{limit} #{offset}
EOS
    end

    def append_where(where)
  	  @where = 
      if @where.nil?
        where 
      else
        "#{@where} AND #{where}" 
      end
    end

    def append_replacable_where(where)
      @tmp_where = where 
    end
  end
end
