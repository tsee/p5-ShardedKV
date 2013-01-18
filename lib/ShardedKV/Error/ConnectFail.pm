package ShardedKV::Error::ConnectFail;
use Moose;
extends 'ShardedKV::Error';

#ABSTRACT: Thrown when connection exceptions occur.

1;
__PACKAGE__->meta->make_immutable(inline_constructor => 0);

__END__

=head1 DESCRIPTION

ShardedKV::Error::ConnectFail is thrown when an exception occurs connecting to
a particular resource. It adds no other attributes beyond what is provided in
the base class: L<ShardedKV::Error>. Please see that module for more
information.

=cut
# vim: ts=2 sw=2 et
