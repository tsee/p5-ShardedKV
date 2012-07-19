package ShardedKV::Storage::Memory;
use Moose;
# ABSTRACT: Testing storage backend for in-memory storage

with 'ShardedKV::Storage';

has 'hash' => (
  is => 'ro',
  isa => 'HashRef',
  default => sub { +{} },
);

sub get {
  my ($self, $key) = @_;
  return $self->{hash}{$key};
}

sub set {
  my ($self, $key, $value_ref) = @_;
  $self->{hash}{$key} = $value_ref;
  return 1;
}

sub delete {
  my ($self, $key) = @_;
  delete $self->{hash}{$key};
  return();
}

# This is a noop for the Memory storage
sub reset_connection { }

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  TODO

=head1 DESCRIPTION

A C<ShardedKV> storage backend that uses a Perl in-memory hash for
storage. It is mainly intended for testing.

Implements the C<ShardedKV::Storage> role.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>

=cut

# vim: ts=2 sw=2 et
