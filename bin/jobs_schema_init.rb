#!/usr/bin/env ruby
require 'optparse'

require File.expand_path("../../lib", __FILE__) + "/etl"

options = {
  :database => "development"
}
OptionParser.new do |opts|
  opts.banner = "Usage: jobs_schema_init.rb [options]"
  opts.on("-f", "--force", "Force deletion and recreation of existing tables") do |o|
    options[:force] = true
  end
  opts.on("-d", "--database NAME", "Specify the database identifier to use (from database.yml)") do |o|
    options[:database] = o
  end
end.parse!

date_puts("Initializing schema for database id '#{options[:database]}'")

conn = Sequel.connect(ETL.db_config[options[:database]])

# Handle case when tables already exist
tables = conn.tables
if tables.length > 0
  if options[:force]
    date_puts("Forcing removal of existing tables...")
    tables.each do |t|
      date_puts("  * Dropping #{t}") 
      conn.drop_table(t)
    end
  else
    date_puts(<<MSG)
This script is cowardly refusing to proceed because the following tables exist:
#{tables.map{|t| "  * #{t}"}.join("\n")}
You can also re-run with the "--force" option to force delete and recreate.
MSG
    exit(0)
  end
end

date_puts("Creating initial schema...")
conn.create_table(:job_run_statuses) do
  primary_key :id
  DateTime :created_at, :null => false
  DateTime :updated_at, :null => false
  String :label, :null => false
  String :name, :null => false  
end

conn.create_table(:jobs) do
  primary_key :id
  DateTime :created_at, null: false
  DateTime :updated_at, null: false
  String :job_class, null: false
  String :job_params, null: false
  String :feed_name, null: false
  String :input_class, null: false
  String :input_params, null: false
end

conn.create_table(:job_runs) do
  primary_key :id
  DateTime :created_at, null: false
  DateTime :updated_at, null: false
  Integer :job_id, :null => false, :index => true
  Integer :job_run_status_id, :null => false, :index => true
  DateTime :queued_at
  DateTime :run_start_time
  DateTime :run_end_time
  Integer :num_rows_success
  Integer :num_rows_error
  String :message
  String :batch
end

# seed data
[
  { label: :queued, name: "Queued" },
  { label: :running, name: "Running" },
  { label: :success, name: "Success" },
  { label: :error, name: "Error" },
].each do |v|
  conn["insert into job_run_statuses (label, name, created_at, updated_at) values (?, ?, ?, ?)", 
    v[:label].to_s, v[:name], DateTime.now, DateTime.now
    ].insert
end

date_puts(<<MSG)
Done! The following tables have been created:
#{conn.tables.map{ |t| "  * #{t}"}.join("\n")}
MSG
