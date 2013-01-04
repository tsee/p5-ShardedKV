package ShardedKV::Error::WriteFail;
use Moose;
extends 'ShardedKV::Error';

has key => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

has operation => (
  is => 'ro',
  isa => Moose::Util::TypeConstraints::enum([qw/set expire/]),
  predicate => 'has_operation'
);

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);
# vim: ts=2 sw=2 et
