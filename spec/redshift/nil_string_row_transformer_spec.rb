require 'etl/redshift/nil_string_row_transformer'

RSpec.describe 'nil_string_row_transformer' do
  describe 'Validate transform' do
    context 'Null should be set to string' do
      let (:test_table) { 'test_table' }

      it 'Convert row, ensure nil is converted to string' do
        t = ETL::Schema::Table.new(:test_table)
        t.string(:name)

        row = { 'name' => nil }
        t = ::ETL::Redshift::NilStringRowTransformer.new({ test_table: t }, '*null*', false)
        result = t.transform(row)
        expect(result.values).to eq(['*null*'])
      end

      it 'Convert row, ensure non nil value is not converted' do
        t = ETL::Schema::Table.new(:test_table)
        t.string(:name)

        row = { 'name' => 'value' }
        t = ::ETL::Redshift::NilStringRowTransformer.new({ test_table: t }, '*null*', false)
        result = t.transform(row)
        expect(result.values).to eq(['value'])
      end

      it 'Convert symbolized row, ensure non nil value is not converted' do
        t = ETL::Schema::Table.new(:test_table)
        t.string(:name)

        row = { name: nil }
        t = ::ETL::Redshift::NilStringRowTransformer.new({ test_table: t }, '*null*', true)
        result = t.transform(row)
        expect(result.values).to eq(['*null*'])
      end

      it 'Convert symbolized row with multiple tables, ensure nil converted' do
        t = ETL::Schema::Table.new(:test_table)
        t.string(:name)
        t2 = ETL::Schema::Table.new('t2')
        t2.string(:info)

        row = { 'test_table' => { name: nil }, 't2' => { info: nil } }
        t = ::ETL::Redshift::NilStringRowTransformer.new({ 'test_table' => t, 't2' => t2 }, '*null*', true)
        result = t.transform(row)
        expect(result.values).to eq([{ name: '*null*' }, { info: '*null*' }])
      end
    end
  end
end
