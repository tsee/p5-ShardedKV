package ShardedKV::Error;
use Moose;
extends 'Throwable::Error';

has storage_type => (
  is => 'ro',
  isa => Moose::Util::TypeConstraints::enum([qw/redis mysql/]),
  required => 1,
);

has endpoint => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);
# vim: ts=2 sw=2 et
