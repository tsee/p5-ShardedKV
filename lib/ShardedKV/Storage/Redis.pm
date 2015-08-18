package ShardedKV::Storage::Redis;
use Moose;
# ABSTRACT: Abstract base class for storing k/v pairs in Redis

use Encode;
use Redis;
use List::Util qw(shuffle);

use ShardedKV::Error::ConnectFail;
use ShardedKV::Error::DeleteFail;

with 'ShardedKV::Storage';

=attribute_public redis_connect_str

A hostname:port string pointing at the Redis for this shard.
Required.

=cut

has 'redis_connect_str' => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

=attribute_public redis_retry_every

The amount of time the C<Redis> object should wait between reconnect attempts, in milliseconds.  Default 500.

=cut

has 'redis_retry_every' => (
  is => 'ro',
  isa => 'Num',
  default => 500,
);

=attribute_public redis_reconnect_timeout

If set, the amount of time the C<Redis> object should try reconnecting for, in seconds.  If 0, do not attempt to reconnect.

=cut

has 'redis_reconnect_timeout' => (
  is => 'ro',
  isa => 'Num',
  default => 0,
);

=attribute_public redis

The C<Redis> object that represents the connection. Will be generated from the
C<redis_connect_str> attribute and may be reset/reconnected at any time.

=cut

has 'redis' => (
  is => 'rw',
  isa => 'Redis',
  lazy => 1,
  builder => '_make_connection',
  clearer => '_clear_connection',
);

=attribute_public expiration_time

Base key expiration time to use in seconds.
Defaults to undef / not expiring at all.

=cut

has 'expiration_time' => ( # in seconds
  is => 'rw',
  isa => 'Num',
);

=attribute_public expiration_time_jitter

Additional random jitter to add to the expiration time.
Defaults to 0. Don't set to undef to disable, set to 0
to disable.

=cut

has 'expiration_time_jitter' => ( # in seconds
  is => 'rw',
  isa => 'Num',
  default => 0,
);


=attribute_public database_number

Indicates the number of the Redis database to use for this shard.
If undef/non-existant, no specific database will be selected,
so the Redis server will use the default.

=cut

has 'database_number' => (
  is => 'rw',
  isa => 'Int',
  trigger => sub {
    my $self = shift;
    $self->redis->select(shift);
  },
);

sub _make_connection {
  my ($self) = @_;
  my $endpoint = $self->redis_connect_str;
  my $r;
  eval {
    $r = Redis->new( # dies if it can't connect!
      server => $endpoint,
      encoding => undef, # no automatic utf8 encoding for performance
      every => $self->redis_retry_every,
      reconnect => $self->redis_reconnect_timeout,
    );
    1;
  } or do {
    my $error = $@ || "Zombie Error";
    ShardedKV::Error::ConnectFail->throw({
      endpoint => $endpoint,
      storage_type => 'redis',
      message => "Failed to make a connection to Redis ($endpoint): $error",
    });
  };
  my $dbno = $self->database_number;
  $r->select($dbno) if defined $dbno;
  return $r;
}

=method_public delete

Implemented in the base class, this method deletes the given key from the Redis shard.

=cut

sub delete {
  my ($self, $key) = @_;
  my $rv;
  eval {
    $rv = $self->redis->del($key);
    1;
  } or do {
    my $error = $@ || "Zombie Error";
    my $endpoint = $self->redis_connect_str;
    $self->reset_connection;
    ShardedKV::Error::DeleteFail->throw({
      endpoint => $endpoint,
      key => $key,
      storage_type => 'redis',
      message => "Failed to delete key ($key) to Redis ($endpoint): $error",
    });
  };
  return $rv ? 1 : 0;
}

=method_public get

Not implemented in the base class. This method is supposed to fetch a value
back from Redis. Beware: Depending on the C<ShardedKV::Storage::Redis> subclass,
the reference type that this method returns may vary. For example, if you use
C<ShardedKV::Storage::Redis::String>, the return value will be a scalar reference
to a string. For C<ShardedKV::String::Redis::Hash>, the return value is
unsurprisingly a hash reference.

=cut

sub get { die "Method get() not implemented in abstract base class" }

=method_public set

The counterpart to C<get>. Expects values as second argument. The value must
be of the same reference type that is returned by C<get()>.

=cut

sub set { die "Method set() not implemented in abstract base class" }

sub reset_connection {
  my ($self) = @_;
  $self->_clear_connection();
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  # use a subclass instead

=head1 DESCRIPTION

This class consumes the L<ShardedKV::Storage> role. It is an abstract base
class for storing key-value pairs in Redis. It does not actually implement
the C<get()> and C<set()> methods and does not impose a Redis value type.
Different subclasses of this class are expected to represent different
storages for distinct Redis value types.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>
* L<ShardedKV::Storage::Redis::String>
* L<ShardedKV::Storage::Redis::Hash>
* L<Redis>
# vim: ts=2 sw=2 et
