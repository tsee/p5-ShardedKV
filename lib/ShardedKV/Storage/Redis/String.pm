package ShardedKV::Storage::Redis::String;
use Moose;
# ABSTRACT: Storing simple string values in Redis
use Encode;
use Redis;

extends 'ShardedKV::Storage::Redis';

sub get {
  my ($self, $key) = @_;
  # fetch from master by default (TODO revisit later)
  my $master = $self->redis_master;
  my $vref = \($master->get($key));
  Encode::_utf8_on($$vref); # FIXME wrong, wrong, wrong, but Redis.pm would otherwise call encode() all the time
  return defined($$vref) ? $vref : undef;
}

sub set {
  my ($self, $key, $value_ref) = @_;
  my $r = $self->redis_master;
  my $expire = $self->expiration_time;

  my $rv = $r->set($key, $$value_ref);

  if (defined $expire) {
    $r->pexpire(
      $key, int(1000*($expire+rand($self->expiration_time_jitter)))
    );
  }

  return $rv;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  use ShardedKV;
  use ShardedKV::Storage::Redis::String;
  ... create ShardedKV...
  my $storage = ShardedKV::Storage::Redis::String->new(
    redis_master_str => 'redisshard1:679',
    expiration_time => 60*60, #1h
  );
  ... put storage into ShardedKV...
  
  # values are scalar references to strings
  $skv->set("foo", \"bar");
  my $value_ref = $skv->get("foo");

=head1 DESCRIPTION

This subclass of L<ShardedKV::Storage::Redis> implements
simple string/blob values in Redis. See the documentation
for C<ShardedKV::Storage::Redis> for the interface of this
class.

The values of a C<ShardedKV::Storage::Redis::String> are
actually scalar references to strings.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>
* L<ShardedKV::Storage::Redis>
* L<ShardedKV::Storage::Redis::Hash>

=cut
