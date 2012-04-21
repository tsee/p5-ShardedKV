use Test::More tests => 6;
BEGIN { use_ok('ShardedKV') };
BEGIN { use_ok('ShardedKV::Continuum::Ketama') };
BEGIN { use_ok('ShardedKV::Storage::MySQL') };
BEGIN { use_ok('ShardedKV::Storage::Redis') };
BEGIN { use_ok('ShardedKV::Storage::Redis::String') };
BEGIN { use_ok('ShardedKV::Storage::Redis::Hash') };
