require 'spec_helper'
require 'etl'

RSpec.describe 'secrets' do
  context 'validate we can get secrets correctly' do
    it 'get secret from a file' do
      begin
        File.write('./secret_file.txt', 'MYSECRET!')
        ENV['SECRET_FILE_PATH'] = './secret_file.txt'
        value_found = ::ETL.config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret')
        ENV.delete('SECRET_FILE_PATH')
        expect(value_found).to eq 'MYSECRET!'
      ensure
        File.delete('./secret_file.txt')
      end
    end
    it 'get secret from env var' do
      ENV['SECRET_TOKEN'] = 'MYSECRET!'
      value_found = ::ETL.config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret')
      ENV.delete('SECRET_TOKEN')
      expect(value_found).to eq 'MYSECRET!'
    end
    it 'Expect secret token env var not specified should error' do
      expect { ::ETL.config.secret_value('SECRET_FILE_PATH', 'SECRET_TOKEN', 'Cannot find secret') } .to raise_error('Cannot find secret')
    end
  end
end
