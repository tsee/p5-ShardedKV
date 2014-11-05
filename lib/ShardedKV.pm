package ShardedKV;
use Moose;
# ABSTRACT: An interface to sharded key-value stores

require ShardedKV::Storage;
require ShardedKV::Storage::Memory;
require ShardedKV::Continuum;

use Carp;

=attribute_public continuum

The continuum object decides on which shard a given key lives.
This is required for a C<ShardedKV> object and must be an object
that implements the C<ShardedKV::Continuum> role.

=cut

has 'continuum' => (
  is => 'rw',
  does => 'ShardedKV::Continuum',
  required => 1,
  trigger => \&_rebuild_getter,
);

=attribute_public migration_continuum

This is a second continuum object that has additional shards configured.
If this is set, a passive key migration is in effect. See C<begin_migration>
below!

=cut

has 'migration_continuum' => (
  is => 'rw',
  does => 'ShardedKV::Continuum',
  trigger => \&_rebuild_getter,
  clearer => '_clear_migration_continuum',
);

sub _rebuild_getter {
    $_[0]->has_getter
      and ${$_[0]->getter} = $_[0]->_build_get_currying();
}

has 'getter' => (
  is => 'ro',
  lazy => 1,
  predicate => 'has_getter',
  clearer => '_clear_getter',
  builder => '_build_getter',
);

sub _build_getter { \( $_[0]->_build_get_currying() ) }

=attribute_public storages

A hashref of storage objects, each of which represents one shard.
Keys in the hash must be the same labels/shard names that are used
in the continuum. Each storage object must implement the
C<ShardedKV::Storage> role.

=cut

has 'storages' => (
  is => 'ro',
  isa => 'HashRef', # of ShardedKV::Storage doing-things
  default => sub { +{} },
);


=attribute_public logger

If set, this must be a user-supplied object that implements
a certain number of methods which are called throughout ShardedKV
for logging/debugging purposes. See L</LOGGING> for details.

=cut

has 'logger' => (
  is => 'rw',
  trigger => \&_rebuild_getter,
);

=method_public get

Given a key, fetches the value for that key from the correct shard
and returns that value or undef on failure.

Different storage backends may return a reference to the value instead.
For example, the Redis and Memory backends return scalar references,
whereas the mysql backend returns an array reference. This might still
change, likely, all backends may be required to return scalar references
in the future.

=cut

# bypassing accessors since this is a hot path
sub get { ${$_[0]->getter}->($_[1]) }

sub _build_get_currying {
  my ($self) = @_;

  my $mig_cont = $self->migration_continuum;
  my $cont     = $self->continuum;
  my $logger   = $self->logger;
  my $do_debug = $logger && $logger->is_debug ? 1 : 0;
  my $storages = $self->storages;

  if (! defined $mig_cont) {
      return sub {
          my ($key) = @_;
          my $where = $cont->choose($key);
          $logger->debug("get() using regular continuum, got storage '$where'") if $do_debug;
          my $storage = $storages->{ $where }
            or croak "Failed to find chosen storage (server) for id '$where' via key '$key'";
          return $storage->get($key);
      };
  }

  return sub {
      my ($key) = @_;
      my $chosen_shard = $mig_cont->choose($key);
      $logger->debug("get() using migration continuum, got storage '$chosen_shard'") if $do_debug;
      my $storage = $storages->{ $chosen_shard }
        or croak "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'";
      my $value_ref = $storage->get($key);
      defined $value_ref
        and return $value_ref;

      # found nothing in the migration continuum, try the normal one
      my $where = $cont->choose($key);
      $logger->debug("get() using regular continuum, got storage '$where'") if $do_debug;

      # we hit the same shard, not useful to try.
      $where eq $chosen_shard
        and return undef;

      $storage = $storages->{ $where }
        or croak "Failed to find chosen storage (server) for id '$where' via key '$key'";
      return $storage->get($key);
  };

}

=method_public set

Given a key and a value, saves the value into the key within the
correct shard.

The value needs to be a reference of the same type that would be
returned by the storage backend when calling C<get()>. See the
discussion above.

=cut

# bypassing accessors since this is a hot path
sub set {
  my ($self, $key, $value_ref) = @_;
  my $continuum = $self->{migration_continuum};
  $continuum = $self->{continuum} if not defined $continuum;

  my $where = $continuum->choose($key);
  my $storage = $self->{storages}{$where};
  $storage
    or croak "Failed to find chosen storage (server) for id '$where' via key '$key'";

  $storage->set($key, $value_ref);
}

=method_public delete

Given a key, deletes the key's entry from the correct shard.

In a migration situation, this might attempt to delete the key from
multiple shards, see below.

=cut

sub delete {
  my ($self, $key) = @_;

  my ($mig_cont, $cont) = @{$self}{qw(migration_continuum continuum)};

  # dumb code for efficiency (otherwise, this would be a loop or in methods)

  my $logger = $self->{logger};
  my $do_debug = ($logger and $logger->is_debug) ? 1 : 0;

  my $storages = $self->{storages};
  my $chosen_shard;
  # Try deleting from shard pointed at by migr. cont. first
  if (defined $mig_cont) {
    $chosen_shard = $mig_cont->choose($key);
    $logger->debug("Deleting from migration continuum, got storage '$chosen_shard'") if $do_debug;
    my $storage = $storages->{ $chosen_shard };
    die "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'"
      if not $storage;
    $storage->delete($key);
  }

  # ALWAYS also delete from the shard pointed at by the main continuum
  my $where = $cont->choose($key);
  $logger->debug("Deleting from continuum, got storage '$where'") if $do_debug;
  if (!$chosen_shard or $where ne $chosen_shard) {
    my $storage = $storages->{ $where };
    die "Failed to find chosen storage (server) for id '$where' via key '$key'"
      if not $storage;
    $storage->delete($key);
  }
}

=method_public reset_connection

Given a key, it retrieves to which shard it would have communicated and calls
reset_connection() upon it. This allows doing a reconnect only for the shards
that have problems. If there is a migration_continuum it will also reset the
connection to that shard as well in an abundance of caution.

=cut

sub reset_connection {
  my ($self, $key) = @_;

  my ($mig_cont, $cont) = @{$self}{qw(migration_continuum continuum)};

  # dumb code for efficiency (otherwise, this would be a loop or in methods)

  my $logger = $self->{logger};
  my $do_debug = ($logger and $logger->is_debug) ? 1 : 0;

  my $storages = $self->{storages};
  my $chosen_shard;
  # Reset the shard pointed at by migr. cont. first
  if (defined $mig_cont) {
    $chosen_shard = $mig_cont->choose($key);
    $logger->debug("Resetting the connection to the shard from migration continuum, got storage '$chosen_shard'") if $do_debug;
    my $storage = $storages->{ $chosen_shard };
    die "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'"
      if not $storage;
    $storage->reset_connection();
  }

  # Reset the shard from the main continuum
  my $where = $cont->choose($key);
  $logger->debug("Resetting the connection to the shard from the main continuum, got storage '$where'") if $do_debug;
  if (!$chosen_shard or $where ne $chosen_shard) {
    my $storage = $storages->{ $where };
    die "Failed to find chosen storage (server) for id '$where' via key '$key'"
      if not $storage;
    $storage->reset_connection();
  }
}

=method_public begin_migration

Given a C<ShardedKV::Continuum> object, this sets the
C<migration_continuum> property of the C<ShardedKV>, thus
beginning a I<passive> key migration. Right now, the only
kind of migration that is supported is I<adding> shards!
Only one migration may be in effect at a time. The
I<passive> qualification there is very significant. If you are,
for example, using the Redis storage backend with a key
expiration of one hour, then you B<know>, that after letting
the passive migration run for one hour, all keys that are
still relevant will have been migrated (or expired if they
were not relevant).

Full migration example:

  use ShardedKV;
  use ShardedKV::Continuum::Ketama;
  use ShardedKV::Storage::Redis;
  
  my $continuum_spec = [
    ["shard1", 100], # shard name, weight
    ["shard2", 150],
  ];
  my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);
  
  # Redis storage chosen here, but can also be "Memory" or "MySQL".
  # "Memory" is for testing. Mixing storages likely has weird side effects.
  my %storages = (
    shard1 => ShardedKV::Storage::Redis->new(
      redis_connect_str => 'redisserver:6379',
      expiration_time => 60*60,
    ),
    shard2 => ShardedKV::Storage::Redis->new(
      redis_connect_str => 'redisserver:6380',
      expiration_time => 60*60,
    ),
  );
  
  my $skv = ShardedKV->new(
    storages => \%storages,
    continuum => $continuum,
  );
  # ... use the skv ...
  
  # Oh, we need to extend it!
  # Add storages:
  $skv->storages->{shard3} = ShardedKV::Storage::Redis->new(
    redis_connect_str => 'NEWredisserver:6379',
    expiration_time => 60*60,
  );
  # ... could add more at the same time...
  my $old_continuum = $skv->continuum;
  my $extended_continuum = $old_continuum->clone;
  $extended_continuum->extend([shard3 => 120]);
  $skv->begin_migration($extended_continuum);
  # ... use the skv normally...
  # ... after one hour (60*60 seconds), we can stop the migration:
  $skv->end_migration();

The logic for the migration is fairly simple:

If there is a migration continuum, then for get requests, that continuum
is used to find the right shard for the given key. If that shard does not
have the key, we check the original continuum and if that points the key
at a different shard, we query that.

For delete requests, we also attempt to delete from the shard pointed to
by the migration continuum AND the shard pointed to by the main continuum.

For set requests, we always only use the shard deduced from the migration
continuum

C<end_migration()> will promote the migration continuum to the regular
continuum and set the C<migration_continuum> property to undef.

=cut

sub begin_migration {
  my ($self, $migration_continuum) = @_;

  my $logger = $self->logger;
  if ($self->migration_continuum) {
    my $err = "Cannot start a continuum migration in the middle of another migration";
    $logger->fatal($err) if $logger;
    croak($err);
  }
  $logger->info("Starting continuum migration") if $logger;

  $self->migration_continuum($migration_continuum);
}

=method_public end_migration

See the C<begin_migration> docs above.

=cut

sub end_migration {
  my ($self) = @_;
  my $logger = $self->logger;
  $logger->info("Ending continuum migration") if $logger;

  $self->continuum($self->migration_continuum);
  $self->_clear_migration_continuum;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  use ShardedKV;
  use ShardedKV::Continuum::Ketama;
  use ShardedKV::Storage::Redis::String;
  
  my $continuum_spec = [
    ["shard1", 100], # shard name, weight
    ["shard2", 150],
  ];
  my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);
  
  # Redis storage chosen here, but can also be "Memory" or "MySQL".
  # "Memory" is for testing. Mixing storages likely has weird side effects.
  my %storages = (
    shard1 => ShardedKV::Storage::Redis::String->new(
      redis_connect_str => 'redisserver:6379',
    ),
    shard2 => ShardedKV::Storage::Redis::String->new(
      redis_connect_str => 'redisserver:6380',
    ),
  );
  
  my $skv = ShardedKV->new(
    storages => \%storages,
    continuum => $continuum,
  );
  
  my $value = $skv->get($key);
  $skv->set($key, $value);
  $skv->delete($key);

=head1 DESCRIPTION

This module implements an abstract interface to a sharded key-value store.
The storage backends as well as the "continuum" are pluggable. "Continuum"
is to mean "the logic that decides in which shard a particular key lives".
Typically, people use consistent hashing for this purpose and very commonly
the choice is to use ketama specifically. See below for references.

Beside the abstract querying interface, this module also implements logic
to add one or more servers to the continuum and use passive key migration
to extend capacity without downtime. Do make it a point to understand the
logic before using it. More on that below.

=head2 LOGGING

ShardedKV allows instrumentation for logging and debugging by setting
the C<logger> attribute of the main ShardedKV object, and/or its
continuum and/or any or all storage sub-objects. If set, the
C<logger> attribute must be an object implementing the following methods:

=for :list
* trace
* debug
* info
* warn
* error
* fatal

which take a string parameter that is to be logged.
These logging levels might be familiar since they are taken from L<Log::Log4perl>,
which means that you can use a C<Log::Log4perl::Logger> object here.

Additionally, the following methods must return whether or not the given log
level is enabled, to potentially avoid costly construction of log messages:

=for :list
* is_trace
* is_debug
* is_info
* is_warn
* is_error
* is_fatal

=head1 SEE ALSO

=for :list
* L<ShardedKV::Storage>
* L<ShardedKV::Storage::Redis>
* L<Redis>
* L<ShardedKV::Storage::Memory>
* L<ShardedKV::Storage::MySQL>
* L<DBI>
* L<DBD::mysql>

=for :list
* L<ShardedKV::Continuum>
* L<ShardedKV::Continuum::Ketama>
* L<Algorithm::ConsistentHash::Ketama>
* L<https://github.com/RJ/ketama>
* L<ShardedKV::Continuum::StaticMapping>

=head1 ACKNLOWLEDGMENT

This module was originally developed for Booking.com.
With approval from Booking.com, this module was generalized
and put on CPAN, for which the authors would like to express
their gratitude.

=cut
# vim: ts=2 sw=2 et
