require 'etl/schema/table'

RSpec.describe 'Table' do
  describe 'Use Table' do
    context 'Can find string columns.' do
      let (:test_table) { 'test_table' }

      it 'Find string columns' do
        t = ETL::Schema::Table.new(:test_table)
        t.string(:name)
        t.int(:house_number)
        t.text(:address)
        t.varchar(:notes, 240)
        t.nvarchar(:country_info, 80)
        t.char(:coding, 10)
        cols = t.string_columns
        expect(cols).to eq(%w[name address notes country_info coding])
      end
    end
  end
end
