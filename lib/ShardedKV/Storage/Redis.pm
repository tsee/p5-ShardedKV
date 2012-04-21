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

sub delete {
  my ($self, $key) = @_;
  return $self->master->delete($key);
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Redis - ...

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

L<ShardedKV>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
