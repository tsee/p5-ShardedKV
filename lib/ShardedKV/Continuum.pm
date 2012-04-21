package ShardedKV::Continuum;
use Moose::Role;

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

=head1 NAME

ShardedKV::Continuum - The continuum role

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

=head1 METHODS YOU NEED TO IMPLEMENT

=head2 choose

Given a key name, must return the name of the shard that
the key lives on.

=head2 clone

Returns a deep copy of the object.

=head2 extend

Given one or multiple shard specifications, adds these to
the continuum.

=head2 serialize

Returns a string that could be used to recreate the continuum.

=head2 deserialize

Given such a string, recreates the exact same continuum.

=head2 get_bucket_names

Returns a list of all shard/bucket names in the continuum.

=head1 SEE ALSO

L<ShardedKV>, L<ShardedKV::Continuum::Ketama>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
