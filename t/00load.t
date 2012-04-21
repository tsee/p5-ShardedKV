use Test::More tests => 1;
use ShardedKV;
use ShardedKV::Continuum::Ketama;
use ShardedKV::Storage::MySQL;
use ShardedKV::Storage::Redis;
use ShardedKV::Storage::Redis::String;
use ShardedKV::Storage::Redis::Hash;

pass();
