package ShardedKV::Storage::Memory;
use Moose;
use Encode;

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

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Memory - ...

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
