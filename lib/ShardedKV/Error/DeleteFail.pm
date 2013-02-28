package ShardedKV::Error::DeleteFail;
use Moose;
extends 'ShardedKV::Error';

#ABSTRACT: Thrown when delete() fails on a storage backend


=attribute_public key

  (is: ro, isa: Str, required)

key holds what particular key was used for the delete() call.

=cut

has key => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);

__END__

=head1 DESCRIPTION

ShardedKV::Error::DeleteFail is an exception thrown when there is a problem
deleting a key from a particular storage backend. The exception will contain
which key failed.

=cut

# vim: ts=2 sw=2 et
