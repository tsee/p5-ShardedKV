package ShardedKV;
use Moose;

require ShardedKV::Storage;
require ShardedKV::Storage::Memory;
require ShardedKV::Continuum;

has 'continuum' => (
  is => 'rw',
  does => 'ShardedKV::Continuum',
  required => 1,
);

has 'migration_continuum' => (
  is => 'rw',
  does => 'ShardedKV::Continuum',
);

has 'storages' => (
  is => 'ro',
  isa => 'HashRef', # of ShardedKV::Storage doing-things
  default => sub { +{} },
);

# bypassing accessors since this is a hot path
sub get {
  my ($self, $key) = @_;
  my ($mig_cont, $cont) = @{$self}{qw(migration_continuum continuum)};

  # dumb code for efficiency (otherwise, this would be a loop or in methods)

  my $storages = $self->{storages};
  my $chosen_shard;
  my $value_ref;
  if (defined $mig_cont) {
    $chosen_shard = $mig_cont->choose($key);
    my $storage = $storages->{ $chosen_shard };
    die "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'"
      if not $storage;
    $value_ref = $storage->get($key);
  }

  if (not defined $value_ref) {
    my $where = $cont->choose($key);
    if (!$chosen_shard or $where ne $chosen_shard) {
      my $storage = $storages->{ $where };
      die "Failed to find chosen storage (server) for id '$where' via key '$key'"
        if not $storage;
      $value_ref = $storage->get($key);
    }
  }

  return $value_ref;
}

# bypassing accessors since this is a hot path
sub set {
  my ($self, $key, $value_ref) = @_;
  my $continuum = $self->{migration_continuum};
  $continuum = $self->{continuum} if not defined $continuum;

  my $where = $continuum->choose($key);
  my $storage = $self->{storages}{$where};
  if (not $storage) {
    die "Failed to find chosen storage (server) for id '$where' via key '$key'";
  }

  $storage->set($key, $value_ref);
}

sub delete {
  my ($self, $key) = @_;

  my $continuum = $self->{migration_continuum};
  $continuum = $self->{continuum} if not defined $continuum;

  my $where = $continuum->choose($key);
  my $storage = $self->{storages}{$where};
  if (not $storage) {
    die "Failed to find chosen storage (server) for id '$where' via key '$key'";
  }

  $storage->delete($key);
}

sub begin_migration {
  my ($self, $migration_continuum) = @_;

  if ($self->migration_continuum) {
    Carp::croak("Cannot start a continuum migration in the middle of another migration");
  }

  $self->migration_continuum($migration_continuum);
}

sub end_migration {
  my ($self) = @_;
  $self->continuum($self->migration_continuum);
  delete $self->{migration_continuum};
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV - An interface to sharded key-value stores

=head1 SYNOPSIS

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
      redis_master_str => 'redisserver:6379',
      redis_slave_strs => ['redisbackup:6379', 'redisbackup2:6379'],
    ),
    shard2 => ShardedKV::Storage::Redis->new(
      redis_master_str => 'redisserver:6380',
      redis_slave_strs => ['redisbackup:6380', 'redisbackup2:6380'],
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

B<This is experimental software. Interfaces and implementation are subject to
change. If you are interested in using this in production, please get in touch
to gauge the current state of stability.>

This module implements an abstract interface to a sharded key-value store.
The storage backends as well as the "continuum" are pluggable. "Continuum"
is to mean "the logic that decides in which shard a particular key lives".
Typically, people use consistent hashing for this purpose and very commonly
the choice is to use ketama specifically. See below for references.

Beside the abstract querying interface, this module also implements logic
to add one or more servers to the continuum and use passive key migration
to extend capacity without downtime. Do make it a point to understand the
logic before using it. More on that below.

=head1 OBJECT ATTRIBUTES

=head2 continuum

The continuum object decides on which shard a given key lives.
This is required for a C<ShardedKV> object and must be an object
that implements the C<ShardedKV::Continuum> role.

=head2 migration_continuum

This is a second continuum object that has additional shards configured.
If this is set, a passive key migration is in effect. See C<begin_migration>
below!

=head2 storages

A hashref of storage objects, each of which represents one shard.
Keys in the hash must be the same labels/shard names that are used
in the continuum. Each storage object must implement the
C<ShardedKV::Storage> role.

=head1 METHODS

=head2 get

Given a key, fetches the value for that key from the correct shard
and returns that value or undef on failure.

Different storage backends may return a reference to the value instead.
For example, the Redis and Memory backends return scalar references,
whereas the mysql backend returns an array reference. This might still
change, likely, all backends may be required to return scalar references
in the future.

=head2 set

Given a key and a value, saves the value into the key within the
correct shard.

The value needs to be a reference of the same type that would be
returned by the storage backend when calling C<get()>. See the
discussion above.

=head2 delete

Given a key, deletes the key's entry from the correct shard.

=head2 begin_migration

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
      redis_master_str => 'redisserver:6379',
      expiration_time => 60*60,
    ),
    shard2 => ShardedKV::Storage::Redis->new(
      redis_master_str => 'redisserver:6380',
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
    redis_master_str => 'NEWredisserver:6379',
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

For set requests, we always only use the shard deduced from the migration
continuum

C<end_migration()> will promote the migration continuum to the regular
continuum and set the C<migration_continuum> property to undef.

=head2 end_migration

See the C<begin_migration> docs above.

=head1 SEE ALSO

L<ShardedKV::Storage>, L<ShardedKV::Storage::Redis>,
L<ShardedKV::Storage::Memory>, L<ShardedKV::Storage::MySQL>

L<ShardedKV::Continuum>, L<ShardedKV::Continuum::Ketama>

L<Algorithm::ConsistentHash::Ketama>, L<https://github.com/RJ/ketama>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
