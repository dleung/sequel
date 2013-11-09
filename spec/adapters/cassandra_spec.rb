SEQUEL_ADAPTER_TEST = :cassandra

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "An Cassandra database" do
  before(:all) do
    # Create a 'test' keyspace environment
    DB.create_keyspace(:default, 
      {replication: {
          class: 'SimpleStrategy',
          replication_factor: 3
        }
      }
    )
    DB.disconnect
    DB = Sequel.connect('cassandra://127.0.0.1', default_keyspace: :default)

    DB.create_table!(:items) do
      text :name, primary_key: true
      int :value
      timestamp :date_created
    end

    DB.create_table!(:books) do
      int :id, primary_key: true
      text :title
      int :category_id
    end

    DB.create_table(:categories) do
      int :id
      text :cat_name
    end

    DB.create_table!(:notes) do
      int :id
      text :title
      text :content
    end
    @d = DB[:items]
  end
end