require 'spec_helper'
require 'etl'

RSpec.describe 'secrets' do
  context 'validate we can get secrets correctly' do
    it 'get secret from a file' do
      begin
        File.write('./secret_file.txt', 'MYSECRET!')
        ENV['SECRET_FILE_PATH'] = './secret_file.txt'
        value_found = ::ETL::Config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret')
        ENV.delete('SECRET_FILE_PATH')
        expect(value_found).to eq 'MYSECRET!'
      ensure
        File.delete('./secret_file.txt')
      end
    end
    it 'get secret from env var' do
      ENV['SECRET_TOKEN'] = 'MYSECRET!'
      value_found = ::ETL::Config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret')
      ENV.delete('SECRET_TOKEN')
      expect(value_found).to eq 'MYSECRET!'
    end
    it 'Expect secret token env var not specified should error' do
      expect { ::ETL::Config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret') } .to raise_error('Cannot find secret')
    end
  end

  context 'validate rabbit mq env vars' do
    it 'Expect rabbitmq values come back correctly' do
      values = ::ETL.config.rabbitmq_env_vars
      expect(values).to eq ({ host: '127.0.0.1', port: 5672, username: 'guest',
                              password: 'guest', heartbeat: 30, vhost: '/', channel_pool_size: 1,
                              prefetch_count: 1, queue: nil })
    end
    it 'Expect queue defaults values come back loading core' do
      ::ETL.config.core_saved = nil
      saved_value = ENV['ETL_CORE_ENVVARS']
      saved_db_password = ENV['ETL_DATABASE_PASSWORD']
      saved_data_dir = ENV['ETL_DATA_DIR']
      begin
        ENV['ETL_CORE_ENVVARS'] = 'TRUE'
        ENV['ETL_DATABASE_PASSWORD'] = 'test'
        ENV['ETL_DATA_DIR'] = './'
        values = ::ETL.config.core
        expect(values[:queue]).to eq ({ class: 'ETL::Queue::File', path: '/var/tmp/etl_queue' })
      ensure
        ::ETL.config.core_saved = nil
        ENV['ETL_CORE_ENVVARS'] = saved_value
        ENV['ETL_DATABASE_PASSWORD'] = saved_db_password
        ENV['ETL_DATA_DIR'] = saved_data_dir
      end
    end
    
    it 'Expect rabbitmq queue values come back core with rabbit set' do
      ::ETL.config.core_saved = nil
      saved_value = ENV['ETL_CORE_ENVVARS']
      saved_queue_class = ENV['ETL_QUEUE_CLASS']
      saved_db_password = ENV['ETL_DATABASE_PASSWORD']
      saved_data_dir = ENV['ETL_DATA_DIR']
      saved_host = ENV['ETL_RABBIT_HOST']
      begin
        ENV['ETL_CORE_ENVVARS'] = 'TRUE'
        ENV['ETL_QUEUE_CLASS'] = '::ETL::Queue::RabbitMQ'
        ENV['ETL_DATABASE_PASSWORD'] = 'test'
        ENV['ETL_DATA_DIR'] = './'
        ENV['ETL_RABBIT_HOST'] = 'foobar'
        values = ::ETL.config.core
        expect(values[:queue]).to eq ({ channel_pool_size: 1,
                                        host: 'foobar', port: 5672,
                                        username: 'guest', password: 'guest',
                                        heartbeat: 30, prefetch_count: 1,
                                        queue: nil,
                                        class: '::ETL::Queue::RabbitMQ',
                                        path: '/var/tmp/etl_queue', vhost: '/' })
      ensure
        ::ETL.config.core_saved = nil
        ENV['ETL_CORE_ENVVARS'] = saved_value
        ENV['ETL_QUEUE_CLASS'] = saved_queue_class
        ENV['ETL_DATABASE_PASSWORD'] = saved_db_password
        ENV['ETL_DATA_DIR'] = saved_data_dir
        ENV['ETL_RABBIT_HOST'] = saved_host
      end
    end
  end
end
