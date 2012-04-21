package ShardedKV::Storage::Redis::Hash;
use Moose;
use parent 'ShardedKV::Storage::Redis';
use Encode;
use Redis;
use Carp ();

sub get {
  my ($self, $key) = @_;
  # fetch from master by default (TODO revisit later)
  my $master = $self->redis_master;
  my %hash = $master->hgetall($key);
  #Encode::_utf8_on($$vref); # FIXME wrong, wrong, wrong, but Redis.pm would otherwise call encode() all the time
  return \%hash;
}

sub set {
  my ($self, $key, $value_ref) = @_;
  if (ref($value_ref) ne 'HASH') {
    Carp::croak("Value must be a hashref");
  }

  my $r = $self->master;
  my $rv = $r->hmset($key, %$value_ref);

  my $expire = $self->expiration_time;
  $r->expire($key, $expire) if $expire;

  return $rv;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Redis::Hash - Storing hash values in Redis

=head1 SYNOPSIS

  use ShardedKV;
  use ShardedKV::Storage::Redis::Hash;
  ... create ShardedKV...
  my $storage = ShardedKV::Storage::Redis::Hash->new(
    redis_master_str => 'redisshard1:679',
    expiration_time => 60*60, #1h
  );
  ... put storage into ShardedKV...
  
  # values are scalar references to strings
  $skv->set("foo", {bar => 'baz', cat => 'dog'});
  my $value_ref = $skv->get("foo");

=head1 DESCRIPTION

This subclass of L<ShardedKV::Storage::Redis> implements
simple string/blob values in Redis. See the documentation
for C<ShardedKV::Storage::Redis> for the interface of this
class.

The values of a C<ShardedKV::Storage::Redis::Hash> are
actually scalar references to strings.

=head1 SEE ALSO

L<ShardedKV>, L<ShardedKV::Storage>
L<ShardedKV::Storage::Redis>, L<ShardedKV::Storage::Redis::String>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
