package ShardedKV::HasLogger;
use strict;
use Moose::Role;
# ABSTRACT: The logging role for ShardedKV objects

=attribute_public logger

If set, this must be a user-supplied object that implements
a certain number of methods which are called throughout ShardedKV
for logging/debugging purposes. See the main documentation for
the ShardedKV module for details.

=cut

has 'logger' => (
  is => 'rw',
  isa => 'Object',
);

no Moose;
1;

__END__

=head1 SYNOPSIS

  use ShardedKV;
  my $skv = ShardedKV->new(
    logger => $logger_obj,
    ...
  );

=head1 DESCRIPTION

This role adds a C<logger> attribute to the consumer. See the main
C<ShardedKV> documentation for details.

This role is consumed by at least the following classes or roles:
C<ShardedKV>, C<ShardedKV::Storage>, C<ShardedKV::Continuum>.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>
* L<ShardedKV::Continuum>
* L<Log::Log4perl>

=cut
