package ShardedKV::Storage::Redis::String;
use Moose;
use parent 'ShardedKV::Storage::Redis';
use Encode;
use Redis;

sub get {
  my ($self, $key) = @_;
  # fetch from master by default (TODO revisit later)
  my $master = $self->redis_master;
  my $vref = \($master->get($key));
  Encode::_utf8_on($$vref); # FIXME wrong, wrong, wrong, but Redis.pm would otherwise call encode() all the time
  return $vref;
}

sub set {
  my ($self, $key, $value_ref) = @_;
  my $r = $self->master;
  my $expire = $self->expiration_time;
  my $rv = $r->set($key, $$value_ref);
  $r->expire($key, $expire) if $expire;
  return $rv;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Redis::String - Storing simple string values in Redis

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

L<ShardedKV>, L<ShardedKV::Storage>
L<ShardedKV::Storage::Redis>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
