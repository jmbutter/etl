require 'etl/redshift/alter_table'
require 'etl/redshift/client'

RSpec.describe 'Alter Table tests' do
  describe 'Alter table operations' do
    context 'Primitive alter table tests' do
      it 'Add column with name and type sql' do
        at = ::ETL::Redshift::AlterTable.new('app_rds', 'table1')
        at.add_column('info_long', 'varchar(255)')
        sql = at.build_sql
        expect(sql).to eq("begin transaction;\nALTER TABLE app_rds.table1 add column info_long varchar(255);\nend transaction;\n")
      end
      it 'Add column with name, type, encoding and not null sql' do
        at = ::ETL::Redshift::AlterTable.new('app_rds', 'table1')
        at.add_column('info_long', 'varchar(255)', "'default_str'", 'lzo', true)
        sql = at.build_sql
        expect(sql).to eq("begin transaction;\nALTER TABLE app_rds.table1 add column info_long varchar(255) DEFAULT 'default_str' ENCODE lzo NOT NULL;\nend transaction;\n")
      end
      it 'Drop column sql' do
        at = ::ETL::Redshift::AlterTable.new('app_rds', 'table1')
        at.drop_column('info_long')
        sql = at.build_sql
        expect(sql).to eq("begin transaction;\nALTER TABLE app_rds.table1 drop column info_long;\nend transaction;\n")
      end
      it 'Rename column sql' do
        at = ::ETL::Redshift::AlterTable.new('app_rds', 'table1')
        at.rename_column('info', 'info_long')
        sql = at.build_sql
        expect(sql).to eq("begin transaction;\nALTER TABLE app_rds.table1 rename column info TO info_long;\nend transaction;\n")
      end
    end
    context 'Test end to end multiple alter tables with client' do
      it 'Add column with name and type sql' do
        client = ETL::Redshift::Client.new(ETL.config.redshift[:test], ETL.config.aws[:test])
        client.execute('DROP TABLE IF EXISTS t2')
        t = ETL::Redshift::Table.new('t2')
        t.int('id')
        t.add_primarykey('id')

        client.create_table(t)

        at1 = ::ETL::Redshift::AlterTable.new('public', 't2')
        at1.add_column('new1', 'varchar(255)')
        at1.add_column('new2', 'varchar(255)')
        at1.execute(client)

        at2 = ::ETL::Redshift::AlterTable.new('public', 't2')
        at2.drop_column('new1')
        at2.rename_column('new2', 'new3')
        at2.execute(client)

        schema = client.table_schema('public', 't2')
        columns = []
        schema.columns.each_key { |c| columns << c }
        expect(columns).to eq(%w[id new3])
      end
    end
  end
end
