package ShardedKV::Storage::Redis::Hash;
use Moose;
# ABSTRACT: Storing hash values in Redis
use Encode;
use Redis;
use Carp ();
use ShardedKV::Error::ReadFail;
use ShardedKV::Error::WriteFail;

extends 'ShardedKV::Storage::Redis';

sub get {
  my ($self, $key) = @_;
  my $redis = $self->redis;
  my $hash;
  eval {
    my @foo = $redis->hgetall($key);
    unless (@foo % 2 == 0) {
      require Data::Dumper;
      die sprintf "PANIC: Should get an even number of keys from hgetall(%s). Got <%s>",
          $key, Data::Dumper->new(\@foo)->Useqq(1)->Terse(1)->Indent(1)->Dump;
    }
    my %foo = @foo;
    if(keys %foo) {
      $hash = \%foo;
    }
    1;
  } or do {
    my $error = $@ || "Zombie Error";
    my $endpoint = $self->redis_connect_str;
    $self->reset_connection;
    ShardedKV::Error::ReadFail->throw({
      endpoint => $endpoint,
      key => $key,
      storage_type => 'redis',
      message => "Failed to fetch key ($key) from Redis ($endpoint): $error",
    });
  };
  return $hash;
}

sub set {
  my ($self, $key, $value_ref) = @_;
  if (ref($value_ref) ne 'HASH') {
    Carp::croak("Value must be a hashref");
  }

  my $r = $self->redis;

  my $rv;
  eval {
    $rv = $r->hmset($key, %$value_ref);
    1;
  } or do {
    my $error = $@ || "Zombie Error";
    my $endpoint = $self->redis_connect_str;
    $self->reset_connection;
    ShardedKV::Error::WriteFail->throw({
      endpoint => $endpoint,
      key => $key,
      storage_type => 'redis',
      operation => 'set',
      message => "Failed to store key ($key) to Redis ($endpoint): $error",
    });
  };

  my $expire = $self->expiration_time;
  if (defined $expire) {
    eval {
      $r->pexpire(
        $key, int(1000*($expire+rand($self->expiration_time_jitter)))
      );
      1;
    } or do {
      my $error = $@ || "Zombie Error";
      my $endpoint = $self->redis_connect_str;
      $self->reset_connection;
      ShardedKV::Error::WriteFail->throw({
        endpoint => $endpoint,
        key => $key,
        storage_type => 'redis',
        operation => 'expire',
        message => "Failed to store key ($key) to Redis ($endpoint): $error",
      });
    };
  }

  return $rv;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  use ShardedKV;
  use ShardedKV::Storage::Redis::Hash;
  ... create ShardedKV...
  my $storage = ShardedKV::Storage::Redis::Hash->new(
    redis_connect_str => 'redisshard1:679',
    expiration_time => 60*60, #1h
  );
  ... put storage into ShardedKV...
  
  # values are HashRefs
  $skv->set("foo", {bar => 'baz', cat => 'dog'});
  my $value_ref = $skv->get("foo");

=head1 DESCRIPTION

This subclass of L<ShardedKV::Storage::Redis> implements
simple string/blob values in Redis. See the documentation
for C<ShardedKV::Storage::Redis> for the interface of this
class.

The values of a C<ShardedKV::Storage::Redis::Hash> are HashRefs.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>
* L<ShardedKV::Storage::Redis>
* L<ShardedKV::Storage::Redis::String>
=cut
# vim: ts=2 sw=2 et
