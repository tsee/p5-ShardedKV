package ShardedKV::Error::WriteFail;
use Moose;
extends 'ShardedKV::Error';

#ABSTRACT: Thrown when set() fails on a storage backend


=public_attribute key

  (is: ro, isa: Str, required)

key holds what particular key was used for the set() call.

=cut

has key => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

=public_attribute operation

  (is: ro, isa: enum(set, expire))

operation may contain what operation the set was doing when the failure
occurred. In the case of the Redis storage backend, the expiration operation is
separate from the actual set operation. In those two cases, this attribute will
be set with the appropriate operation. Other backends may or may not supply
this value.

=cut

=public_method has_operation

has_operation() is the predicate check for the L</operation> attribute. It
checks if operation is defined (ie. the backend set a value).

=cut

has operation => (
  is => 'ro',
  isa => Moose::Util::TypeConstraints::enum([qw/set expire/]),
  predicate => 'has_operation'
);

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);

__END__

=head1 DESCRIPTION

ShardedKV::Error::WriteFail is an exception thrown when there is a problem
writing to the particular storage backend. The exception will contain which key
failed, and potentially which operation during the set() failed.

=cut

# vim: ts=2 sw=2 et
