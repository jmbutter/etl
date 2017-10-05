require 'clamp'
require 'singleton'

module ETL::Cli
  class CommandExtensionManager
    include Singleton

    attr_reader :commands

    def initialize
      @commands = {}
    end

    def register(name, description, cmd_klass)
      ETL.logger.debug("Registering command extension with manager:#{name}, #{cmd_klass}")
      if @commands.has_key?(name)
        ETL.logger.warn("Overwriting previous registration of: #{name} => #{@commands[name]}")
      end

      @commands[name] = { description: description, command: cmd_klass }
    end
  end
end
