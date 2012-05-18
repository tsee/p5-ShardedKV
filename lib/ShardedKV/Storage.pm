package ShardedKV::Storage;
use Moose::Role;
# ABSTRACT: Role for classes implementing storage backends

with 'ShardedKV::HasLogger';

=role_require get

TODO

=role_require set

TODO

=role_require delete

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

