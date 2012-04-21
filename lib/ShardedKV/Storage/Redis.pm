package ShardedKV::Storage::Redis;
use Moose;
use Encode;
use Redis;
use List::Util qw(shuffle);

with 'ShardedKV::Storage';

has 'redis_master_str' => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

# For either failover or reading => TODO might make sense to separate the two
has 'redis_slave_strs' => (
  is => 'ro',
  isa => 'ArrayRef[Str]',
  required => 1,
  default => sub {[]},
);

has 'redis_master' => (
  is => 'rw',
  isa => 'Redis',
  lazy => 1,
  builder => '_make_master_conn',
);

has 'expiration_time' => ( # in seconds
  is => 'rw',
  #isa => 'Num',
);

sub _make_connection {
  my ($self, $endpoint) = @_;
  my $r = Redis->new( # dies if it can't connect!
    server => $endpoint,
    encoding => undef, # no automatic utf8 encoding for performance
  );
  return $r;
}

sub _make_master_conn {
  my $self = shift;
  return $self->_make_connection($self->redis_master_str);
}

sub _make_slave_conn {
  my $self = shift;
  my $conn;
  foreach my $slave (shuffle(@{$self->redis_slave_strs})) {
    last if eval {
      $conn = $self->_make_connection($slave);
    };
  }
  die if not $conn;
  return $conn;
}

sub delete {
  my ($self, $key) = @_;
  return $self->master->delete($key);
}

sub get { die "Method get() not implemented in abstract base class" }
sub set { die "Method set() not implemented in abstract base class" }

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Redis - Abstract base class for storing k/v pairs in Redis

=head1 SYNOPSIS

  # use a subclass instead

=head1 DESCRIPTION

This class consumes the L<ShardedKV::Storage> role. It is an abstract base
class for storing key-value pairs in Redis. It does not actually implement
the C<get()> and C<set()> methods and does not impose a Redis value type.
Different subclasses of this class are expected to represent different
storages for distinct Redis value types.

=head1 OBJECT ATTRIBUTES

=head2 redis_master_str

A hostname:port string pointing at the Redis master for this shard.
Required.

=head2 redis_slave_strs

Array reference of hostname:port strings representing Redis slaves
of the master. Currently unused, will eventually be used for either
reading or master failover.

=head2 redis_master

The C<Redis> object that represents the master connection. Will be
generated from the C<redis_master_str> attribute and may be reset/reconnected
at any time.

=head2 expiration_time

Key expiration time to use in seconds.

=head1 METHODS

=head2 delete

Implemented in the base class, this method deletes the given key from the Redis shard.

=head2 get

Not implemented in the base class. This method is supposed to fetch a value
back from Redis. Beware: Depending on the C<ShardedKV::Storage::Redis> subclass,
the reference type that this method returns may vary. For example, if you use
C<ShardedKV::Storage::Redis::String>, the return value will be a scalar reference
to a string.

=head2 set

The counterpart to C<get>. Expects values as second argument. The value must
be of the same reference type that is returned by C<get()>.

=head1 SEE ALSO

L<ShardedKV>, L<ShardedKV::Storage>, L<ShardedKV::Storage::Redis::String>

L<Redis>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
