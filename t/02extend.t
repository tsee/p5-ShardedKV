use strict;
use warnings;
use Test::More;
use ShardedKV;
use ShardedKV::Continuum::Ketama;

use lib qw(t/lib);
use ShardedKV::Test;

extension_test_by_one_server_ketama(sub {ShardedKV::Storage::Memory->new()}, 'memory');
extension_test_by_multiple_servers_ketama(sub {ShardedKV::Storage::Memory->new()}, 'memory');

done_testing();

