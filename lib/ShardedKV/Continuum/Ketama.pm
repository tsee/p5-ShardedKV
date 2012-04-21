package ShardedKV::Continuum::Ketama;
use Moose;
use Algorithm::ConsistentHash::Ketama;
use JSON::XS qw(encode_json decode_json);

with 'ShardedKV::Continuum';

has '_orig_continuum_spec' => (
  is => 'ro',
);

has '_ketama' => (
  is => 'ro',
  isa => 'Algorithm::ConsistentHash::Ketama',
);

sub choose {
  $_[0]->_ketama->hash($_[1])
}

sub serialize { encode_json( $_[0]->_orig_continuum_spec ) }

sub deserialize {
  my $class = shift;
  return $class->new(from => decode_json( $_[1] ));
}

sub clone {
  my $self = shift;
  return ref($self)->new(from => $self->_orig_continuum_spec);
}

sub extend {
  my $self = shift;
  my $spec = shift;

  my $ketama = $self->_ketama;
  Carp::croak("Ketama spec must be an Array of Arrays, each inner record holding key and weight! This is not an array")
    if not ref($spec) eq 'ARRAY';
  foreach my $elem (@$spec) {
    Carp::croak("Ketama spec must be an Array of Arrays, each inner record "
                . "holding key and weight! This particular record is not an array or does not hold two elements")
      if not ref($elem) eq 'ARRAY' and @$elem == 2;
    $ketama->add_bucket(@$elem);
  }
}

sub get_bucket_names {
  my $self = shift;
  my $ketama = $self->_ketama;
  my @buckets = $ketama->buckets;
  return map $_->label, @buckets;
}

sub BUILD {
  my $self = shift;
  my $args = shift;
  my $from = delete $args->{from};
  if (ref($from) eq 'ARRAY') {
    $self->{_ketama} = $self->_make_ketama($from);
    $self->{_orig_continuum_spec} = $from;
  }
  else {
    die "Invalid 'from' specification for " . __PACKAGE__;
  }
}

sub _make_ketama {
  my $self = shift;
  my $spec = shift;
  my $ketama = Algorithm::ConsistentHash::Ketama->new;
  Carp::croak("Ketama spec must be an Array of Arrays, each inner record holding key and weight! This is not an array")
    if not ref($spec) eq 'ARRAY';
  foreach my $elem (@$spec) {
    Carp::croak("Ketama spec must be an Array of Arrays, each inner record "
                . "holding key and weight! This particular record is not an array or does not hold two elements")
      if not ref($elem) eq 'ARRAY' and @$elem == 2;
    $ketama->add_bucket(@$elem);
  }
  return $ketama;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Continuum::Ketama - Continuum implementation based on ketama consistent hashing

=head1 SYNOPSIS

  use ShardedKV;
  use ShardedKV::Continuum::Ketama;
  my $skv = ShardedKV->new(
    continuum => ShardedKV::Continuum::Ketama->new(
      from => [ [shard1 => 100], [shard2 => 200], ... ],
    ),
    storages => {...},
  );

=head1 DESCRIPTION

A continuum implementation based on ketama consistent hashing.
See C<Algorithm::ConsistentHash::Ketama>.

=head1 SEE ALSO

L<ShardedKV>, L<ShardedKV::Continuum>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
