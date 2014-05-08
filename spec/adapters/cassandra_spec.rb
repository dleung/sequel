SEQUEL_ADAPTER_TEST = :cassandra

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe Sequel::Database do
  before(:each) do
    DB.create_keyspace(:default, 
      {replication: {
          class: 'SimpleStrategy',
          replication_factor: 3
        },
        if_not_exists: true
      }
    )
    DB.disconnect
    DB.connect({host: '127.0.0.1', default_keyspace: :default})
    @db = DB
  end
  
  after(:each) do
    DB.drop_keyspace(:default, {
      if_exists: true
    })
  end
  
  specify "create and drop drop keyspace successfully" do
    # Empty block will test the before(:each) and after(:each) for keyspace
    # creation and removal
  end

  specify "create and drop table successfully" do
    @db.create_table!(:items) {primary_key :id; Integer :number}
    @db.drop_table(:items)
  end
end