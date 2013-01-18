package ShardedKV::Storage;
use Moose::Role;
# ABSTRACT: Role for classes implementing storage backends

with 'ShardedKV::HasLogger';

=role_require get

get() needs to accept a key of some sort and return whatever is relevant.

=role_require set

set() needs to accept both a key, and a reference to a datastructure suitable for storing

=role_require delete

delete() needs to accept a key and it must remove the data stored under that key

=role_require reset_connection

Storage backends must implement reset_connection() to allow for reconnects.
Since most things are not reentrant and signals can mess with the state of
sockets and such, the ability to reset the connection (whatever that means for
your particular storage backend), is paramount. 

=cut

requires qw(get set delete reset_connection);

no Moose;

1;

__END__

=head1 SYNOPSIS

    package ShardedKV::Storage::MyBackend;
    use Moose;
    with 'ShardedKV::Storage';

    sub get { ... }
    sub set { ... }
    sub delete { ... }
    sub reset_connection { ... }
    1;

=head1 DESCRIPTION

ShardedKV::Storage provides a role/interface that storage backends must
consume. Consuming the role requires implementing the three important
operations necessary for a storage backend. There are a few storage backends
that come with ShardedKV. Please see those modules for their specific details.

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage::Memory>
* L<ShardedKV::Storage::Redis>
* L<ShardedKV::Storage::MySQL>

=cut

