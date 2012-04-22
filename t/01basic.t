use strict;
use warnings;
use Test::More;
use ShardedKV;
use ShardedKV::Continuum::Ketama;
use ShardedKV::Storage::MySQL;
use ShardedKV::Storage::Redis::String;

use lib qw(t/lib);
use ShardedKV::Test;

subtest "memory storage" => sub {
  simple_test_one_server_ketama(sub {ShardedKV::Storage::Memory->new()});
  simple_test_five_servers_ketama(sub {ShardedKV::Storage::Memory->new()});
};

subtest "mysql storage" => sub {
  my @conn_args = get_mysql_conf();
  if (not @conn_args) {
    plan skip_all => 'No MySQL connection info, skipping mysql tests';
  }
  else {
    pass("Got MySQL connection details");
  }

  simple_test_one_server_ketama(\&mysql_storage);
  simple_test_five_servers_ketama(\&mysql_storage);
};

subtest "redis string storage" => sub {
  my @conn_args = get_redis_conf();
  if (not @conn_args) {
    plan skip_all => 'No Redis connection info, skipping Redis tests';
  }
  else {
    pass("Got Redis connection details");
  }

  simple_test_one_server_ketama(\&redis_string_storage);
  simple_test_five_servers_ketama(\&redis_string_storage);
};


done_testing();
