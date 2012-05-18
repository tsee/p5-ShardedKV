package ShardedKV::Continuum;
use Moose::Role;
# ABSTRACT: The continuum role

with 'ShardedKV::HasLogger';

=role_require choose

Given a key name, must return the name of the shard that
the key lives on.

=role_require clone

Returns a deep copy of the object.

=role_require extend

Given one or multiple shard specifications, adds these to
the continuum.

=role_require serialize

Returns a string that could be used to recreate the continuum.

=role_require deserialize

Given such a string, recreates the exact same continuum.

=role_require get_bucket_names

Returns a list of all shard/bucket names in the continuum.

=cut

requires qw(
  choose
  clone
  extend
  serialize
  deserialize
  get_bucket_names
);

no Moose;

1;

__END__

=head1 SYNOPSIS

  package ShardedKV::Continuum::MyAlgorithm;
  use Moose;
  with 'ShardedKV::Continuum';
  ... implement necessary methods here ...
  1;

=head1 DESCRIPTION

A class that consumes this role and implements all required
methods correctly can be used as a sharding algorithm for a L<ShardedKV>.

See L<ShardedKV::Continuum::Ketama> for an example.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Continuum::Ketama>
* L<ShardedKV::Continuum::StaticMapping>

=cut
