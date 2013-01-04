package ShardedKV::Error::ConnectFail;
use Moose;
extends 'ShardedKV::Error';

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);
# vim: ts=2 sw=2 et
