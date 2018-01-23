libdir = File.expand_path("..", __FILE__)
$LOAD_PATH.unshift(libdir)

# Pre-define the module so we can use simpler syntax
module ETL
end

require 'sequel'

# Core classes
require 'etl/exception'

# Utilities
require 'etl/util/logger'
require 'etl/util/metrics'
require 'etl/util/hash_util'
require 'etl/util/string_util'
require 'etl/batch'

# Models
require 'etl/models/job_run'
require 'etl/models/job_run_repository'

::ETL::Model::JobRunRepository.instance = ::ETL::Model::JobRunRepository.new

require 'etl/schema/table'
require 'etl/schema/column'

require 'etl/job/result'
require 'etl/job/base'
require 'etl/job/manager'

base_file = 'base.rb'
%w( input output transform queue batch_factory schedule ).each do |d|
  dir = "#{libdir}/#{d}"
  require "#{dir}/#{base_file}"
  Dir.new(dir).each do |file|
    next unless file =~ /\.rb$/
    next if file == base_file
    require "#{dir}/#{file}"
  end
end

module ETL
  # Generic App-wide logger
  def ETL.logger
    @@logger ||= ETL.create_logger
  end

  # Sets generic App-wide logger
  def ETL.logger=(v)
    @@logger = v
  end

  # Creates a new logger instance that we can use for different contexts
  # based on context that is passed in
  def ETL.create_logger(context = {})
    log = ETL.create_class(:log)
    log.context = context.dup
    log
  end

  def ETL.create_metrics
    ETL.create_class(:metrics)
  end

  def ETL.queue
    @@queue ||= ETL.create_queue
  end

  def ETL.queue=(v)
    @@queue = v
  end

  def ETL.create_queue
    queue = ETL.create_class(:queue)
    queue_pauser = ETL.config.core[:queue][:queue_pauser]
    queue.dequeue_pauser = Object::const_get(queue_pauser) unless queue_pauser.nil?
    queue
  end

  # Helper function to create a class given a class name stored in the config
  # under "sym"
  def ETL.create_class(sym)
    cfg = ETL.config.core[sym]
    Object::const_get(cfg[:class]).new(cfg)
  end

  def ETL.load_user_commands
    cl = ClassLoader.new("Loading user command classes:", "_command.rb")
    cl.load(ETL.user_dirs)
  end

  # load all user job classes
  def ETL.load_user_classes
    cl = ClassLoader.new("Loading user file class:")
    cl.load(ETL.user_dirs)
  end

  private
  def ETL.user_dirs
    dirs = []
    if c  = ETL.config.core.fetch(:default, {})[:class_dir]
      dirs << c
    end
    if c = ETL.config.core[:job][:class_dir]
      dirs << c
    end
    dirs
  end

  class ClassLoader
    def initialize(add_message, optional_ends_with = nil)
      @add_message = add_message
      @optional_ends_with = optional_ends_with
    end
    def load(starting_dirs)
      @dirs_map = {}
      starting_dirs.each do |dir|
        find_dirs(dir)
      end
      @dirs_map.each do |dir_path, _value|
        load_classes_in_dir(dir_path)
      end
    end

    # loading the sub directories of the supplied base directory. Adding dirs to hash in case there are duplicates to remove them
    def find_dirs(dir)
        unless dir.start_with?("/")
          dir = ETL.root + "/" + dir
        end
        Dir.entries(dir).select {|entry| File.directory? File.join(dir ,entry) and !(entry =='.' || entry == '..') }.each  do | f|
          @dirs_map["#{dir}/#{f}"] = true
        end
        @dirs_map[dir] = true
    end

    def load_classes_in_dir(class_dir)
      ::Dir.new(class_dir).each do |file|
        next unless file =~ /\.rb$/
        path = class_dir + "/" + file
        load_class(path)
      end
    end

    def load_class(file_path)
      return if !@optional_ends_with.nil? && !file_path.end_with?(@optional_ends_with)
      ETL.logger.debug("#{@add_message} #{file_path}")
      require file_path
    end
  end
end
