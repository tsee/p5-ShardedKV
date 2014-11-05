#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Bin);

use lib "$Bin/../lib";

use Benchmark::Forking qw(cmpthese);

use ShardedKV;
use ShardedKV::Continuum::Ketama;
use ShardedKV::Storage::Memory;

my $continuum_spec = [
    [shard1 => 100], # shard name, weight
];
my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);
my %storages = (
    shard1 => ShardedKV::Storage::Memory->new(
      redis_connect_str => '127.0.0.1:6379',
    ),
);

my $skv = ShardedKV->new(
    storages => \%storages,
    continuum => $continuum,
);
 
my $key = 'foo';
my $value = \'bar';

$skv->set($key, $value);
$skv->get($key);
my $getter = $skv->getter();
$skv->set($key, $value);
cmpthese(-5, {
    'wit_acc' => sub { $skv->normal_get($key) },
    'no_acc' => sub { $skv->get_no_accessors($key) },
    'indirect_getter' => sub { $skv->get($key) },
    'direct_getter' => sub { $$getter->($key) },
});




# This monkeypatching is for reference, this is what would be the get() method
# if we were using accessors

sub ShardedKV::normal_get {
  my ($self, $key) = @_;
  my $mig_cont = $self->migration_continuum;
  my $cont = $self->continuum;

  # dumb code for efficiency (otherwise, this would be a loop or in methods)

  my $logger = $self->logger;
  my $do_debug = $logger && $logger->is_debug ? 1 : 0;

  my $storages = $self->storages;
  my $chosen_shard;
  my $value_ref;
  if (defined $mig_cont) {
    $chosen_shard = $mig_cont->choose($key);
    $logger->debug("get() using migration continuum, got storage '$chosen_shard'") if $do_debug;
    my $storage = $storages->{ $chosen_shard };
    die "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'"
      if not $storage;
    $value_ref = $storage->get($key);
  }

  if (not defined $value_ref) {
    my $where = $cont->choose($key);
    $logger->debug("get() using regular continuum, got storage '$where'") if $do_debug;
    if (!$chosen_shard or $where ne $chosen_shard) {
      my $storage = $storages->{ $where };
      die "Failed to find chosen storage (server) for id '$where' via key '$key'"
        if not $storage;
      $value_ref = $storage->get($key);
    }
  }

  return $value_ref;
}


# This monkeypatching is for reference, this is the implementation bypassing accesors
use Carp;

sub ShardedKV::get_no_accessors {
  my ($self, $key) = @_;
  my ($mig_cont, $cont) = @{$self}{qw(migration_continuum continuum)};

  # dumb code for efficiency (otherwise, this would be a loop or in methods)

  my $logger = $self->{logger};
  my $do_debug = ($logger and $logger->is_debug) ? 1 : 0;

  my $storages = $self->{storages};
  my $chosen_shard;
  my $value_ref;
  if (defined $mig_cont) {
    $chosen_shard = $mig_cont->choose($key);
    $logger->debug("get() using migration continuum, got storage '$chosen_shard'") if $do_debug;
    my $storage = $storages->{ $chosen_shard };
    croak "Failed to find chosen storage (server) for id '$chosen_shard' via key '$key'"
      if not $storage;
    $value_ref = $storage->get($key);
  }

  if (not defined $value_ref) {
    my $where = $cont->choose($key);
    $logger->debug("get() using regular continuum, got storage '$where'") if $do_debug;
    if (!$chosen_shard or $where ne $chosen_shard) {
      my $storage = $storages->{ $where };
      croak "Failed to find chosen storage (server) for id '$where' via key '$key'"
        if not $storage;
      $value_ref = $storage->get($key);
    }
  }

  return $value_ref;
}

