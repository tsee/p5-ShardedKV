package ShardedKV::Storage;
use Moose::Role;
# ABSTRACT: Role for classes implementing storage backends

=role_requires get

TODO

=role_requires set

TODO

=role_requires delete

TODO

=cut

requires qw(get set delete);

no Moose;

1;

__END__

=head1 SYNOPSIS

  TODO

=head1 DESCRIPTION

TODO

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage::Memory>
* L<ShardedKV::Storage::Redis>
* L<ShardedKV::Storage::MySQL>

=cut

