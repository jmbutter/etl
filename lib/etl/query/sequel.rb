module ETL::Query

  # Class that contains shared logic for writing to relational DBs. DB-specific
  # logic should be minimized and put into subclasses.
  class Sequel
    attr_accessor :where, :group_by, :limit, :offset

    # select : list of strings
    # from : string
    # where : list of strings
    # group_by : list of strings
    # limit : int
    def initialize(select, from, where = nil, group_by = nil, limit = nil)
      if !select.is_a?(Array)
        raise "Select is not array"
      elsif !from.is_a?(String)
        raise "From is not string"
      elsif !where.nil? && !where.is_a?(Array)
        raise "Where is not array"
      elsif !group_by.nil? && !group_by.is_a?(Array)
        raise "Group_by is not array"
      elsif !limit.nil? && !limit.is_a?(Integer)
        raise "Limit is not Integer"
      end
        
      @select = select.delete_if(&:empty?)
      if @select.empty?
        raise "Select is empty"
      end

      if from.empty?
        raise "From is empty"
      end
      @from = from 
      @where = if where.nil?
                 where
               else 
                 where.delete_if(&:empty?) 
               end
      @group_by = if group_by.nil?
                    group_by
                  else
                    group_by.delete_if(&:empty?)
                  end
      @limit = limit 
      @offset = nil
    end

    def query
      select = @select.join(", ")
      where = 
        if @where.nil? || @where.empty?
          if !@tmp_where.nil? && !@tmp_where.empty?
            " WHERE #{@tmp_where.join(" AND ")}"
          else
            ""
          end
        else
          if !@tmp_where.nil? && !@tmp_where.empty?
            " WHERE #{@where.join(" AND ")} AND #{@tmp_where.join(" AND ")}"
          else
            " WHERE #{@where.join(" AND ")}"
          end
        end
        
      group_by = 
        if @group_by.nil? || @group_by.empty?
          ""
        else
          " GROUP BY #{@group_by.join(", ")}"
        end

      limit =
        if @limit.nil?
          ""
        else
          " LIMIT #{@limit}"
        end

      if @offset.nil?
        offset = ""
      else
        if limit.empty?
          limit = " LIMIT #{@offset}"
        end
        offset = " OFFSET #{@offset}"
      end

      "SELECT #{select} FROM #{@from}#{where}#{group_by}#{limit}#{offset}"
    end

    # parameter should be array
    def append_where(where)
      raise "Parameter is not Array" if !where.is_a?(Array)

  	  @where = 
      if @where.nil?
        where 
      else
        @where + where 
      end
    end

    # parameter should be array
    def append_replacable_where(where)
      raise "Parameter is not Array" if !where.is_a?(Array)
      @tmp_where = where 
    end

    def set_offset(offset)
      raise "Parameter is not Integer" if !offset.is_a?(Integer)
      @offset = offset 
    end

    def cancel_offset
      @offset = nil 
    end
  end
end
