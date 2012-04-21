use strict;
use warnings;
use Test::More;
use ShardedKV;
use ShardedKV::Continuum::Ketama;

use lib qw(t/lib);
use ShardedKV::Test;

simple_test_one_server_ketama(sub {ShardedKV::Storage::Memory->new()});

simple_test_five_servers_ketama(sub {ShardedKV::Storage::Memory->new()});


done_testing();
