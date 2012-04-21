package # hide from indexers
  ShardedKV::Test;
use strict;
use warnings;
use Test::More;

require Exporter;
our @ISA = qw(Exporter);

our @EXPORT = qw(
  get_mysql_conf
  mysql_connect_hook
  mysql_storage

  test_setget
  simple_test_one_server_ketama
  simple_test_five_servers_ketama

  extension_test_by_one_server_ketama
  extension_test_by_multiple_servers_ketama
  make_skv
);

my @connect_args;
my $file = 'testmysqldsn.conf';
sub get_mysql_conf {
  return @connect_args if @connect_args;

  if (-f $file) {
    open my $fh, "<", $file or die $!;
    @connect_args = <$fh>;
    chomp $_ for @connect_args;
    die "Failed to read DSN" if not @connect_args;
    return @connect_args;
  }

  note("There are not connection details.");
  return();
}

my $shared_connection;
sub mysql_connect_hook {
  return $shared_connection if $shared_connection;
  require DBI;
  require DBD::mysql;
  return( $shared_connection = DBI->connect(get_mysql_conf()) );
}

SCOPE: {
  my $itable;
  sub mysql_storage {
    $itable ||= 1;
    my $table_name = "KVShardTable_$itable";
    note("Creating test shard table $table_name");
    my $st = ShardedKV::Storage::MySQL->new(
      mysql_master_connector => \&mysql_connect_hook,
      table_name => $table_name,
    );
    $st->prepare_table or die "Failed to set up shard table for shard $itable";
    $itable++;
    return $st;
  }
}

sub test_setget {
  my ($name, $skv) = @_;

  is_deeply($skv->get("virgin"), undef, $name);
  $skv->set("foo", \"bar");
  is_deeply($skv->get("foo"), \"bar", $name);
  is_deeply($skv->get("foo"), \"bar", $name);
  $skv->set("foo", \"bar2");
  is_deeply($skv->get("foo"), \"bar2", $name);
  $skv->delete("foo");
  is_deeply($skv->get("foo"), undef, $name);
  is_deeply($skv->get("virgin"), undef, $name);

  srand(0);
  my %data = map {(rand(), rand())} 0..1000;

  foreach (sort keys %data) {
    $skv->set($_, \$data{$_});
  }
  foreach (reverse sort keys %data) {
    is_deeply( $skv->get($_), \$data{$_}, $name );
  }
}

sub test_setget_mysql {
  my ($name, $skv) = @_;

  is_deeply($skv->get("virgin"), undef, $name);
  $skv->set("foo", ["bar"]);
  is_deeply($skv->get("foo"), ["bar"], $name);
  is_deeply($skv->get("foo"), ["bar"], $name);
  $skv->set("foo", ["bar2"]);
  is_deeply($skv->get("foo"), ["bar2"], $name);
  $skv->delete("foo");
  is_deeply($skv->get("foo"), undef, $name);
  is_deeply($skv->get("virgin"), undef, $name);

  srand(0);
  # using a 16 byte key by default.
  my %data = map {(substr(rand(), 0, 16), rand())} 0..1000;

  foreach (sort keys %data) {
    $skv->set($_, [$data{$_}]);
  }
  foreach (reverse sort keys %data) {
    is_deeply( $skv->get($_), [$data{$_}], $name );
  }
}


sub simple_test_one_server_ketama {
  my $storage_maker = shift;

  require ShardedKV::Continuum::Ketama;
  my $continuum_spec = [
    ["server1", 100],
  ];
  my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);

  my $skv = ShardedKV->new(
    storages => {},
    continuum => $continuum,
  );
  foreach (@$continuum_spec) {
    $skv->storages->{$_->[0]} = $storage_maker->();#ShardedKV::Storage::Memory->new();
  }

  isa_ok($skv, "ShardedKV");
  isa_ok($skv->continuum, "ShardedKV::Continuum::Ketama");
  is(ref($skv->storages), "HASH");
  #isa_ok($_, "ShardedKV::Storage::Memory") foreach values %{$skv->storages};

  if (grep $_->isa("ShardedKV::Storage::MySQL"), values %{$skv->storages}) {
    test_setget_mysql("one server mysql", $skv);
  } else {
    test_setget("one server", $skv);
  }
}

sub simple_test_five_servers_ketama {
  my $storage_maker = shift;

  my $continuum_spec = [
    ["server1", 100],
    ["server2", 150],
    ["server3", 200],
    ["server4", 15],
    ["server5", 120],
  ];
  my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);

  my $skv = ShardedKV->new(
    storages => {},
    continuum => $continuum,
  );
  foreach (@$continuum_spec) {
    $skv->storages->{$_->[0]} = $storage_maker->();
  }

  isa_ok($skv, "ShardedKV");
  isa_ok($skv->continuum, "ShardedKV::Continuum::Ketama");
  is(ref($skv->storages), "HASH");
  #isa_ok($_, "ShardedKV::Storage::Memory") foreach values %{$skv->storages};

  my $is_mem = (values(%{$skv->storages}))[0]->isa("ShardedKV::Storage::Memory");
  if ($is_mem) {
    test_setget("five servers", $skv);
  } else { # mysql
    test_setget_mysql("five servers mysql", $skv);
  }

  if ($is_mem) {
    my $servers_with_keys = 0;
    foreach my $server (values %{$skv->{storages}}) {
      # Breaking encapsulation, since we know it's of type Memory
      $servers_with_keys++ if keys %{$server->hash};
    }
    ok($servers_with_keys > 1); # technically probabilistic, but chances of failure are nil
  }
}

sub extension_test_by_one_server_ketama {
  my $storage_maker = shift;
  my $storage_type = shift;

  # yes, yes, this blows.
  my $make_ref = $storage_type =~ /memory/i ? sub {\$_[0]} : sub {[$_[0]]};

  my $continuum_spec = [
    ["server1", 100],
    ["server2", 150],
    ["server3", 200],
  ];

  my $skv = make_skv($continuum_spec, $storage_maker);
  my @keys = (1..1000);
  $skv->set($_, $make_ref->("v$_")) for @keys;
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;

  # Setup new server and an extended continuum
  $skv->storages->{server4} = $storage_maker->();
  my $new_cont = $skv->continuum->clone;
  $new_cont->extend([
    ["server4", 120],
  ]);
  isa_ok($new_cont, "ShardedKV::Continuum::Ketama");

  # set continuum
  $skv->begin_migration($new_cont);
  isa_ok($skv->migration_continuum, "ShardedKV::Continuum::Ketama");

  # Check that reads still work and return the old values
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;

  # Rewrite part of the keys
  my @first_half_keys = splice(@keys, 0, int(@keys/2));
  $skv->set($_, $make_ref->("N$_")) for @first_half_keys;

  # Check old and new keys
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;
  is_deeply($skv->get($_), $make_ref->("N$_")) for @first_half_keys;

  if ($storage_type =~ /memory/i) {
    # FIXME support this part of the test for mysql!
    check_old_new("Single new server", $skv, qr/^server4$/);
  }

  $skv->end_migration;

  ok(!defined($skv->migration_continuum),
     "no migration continuum after migration end");
}

sub extension_test_by_multiple_servers_ketama {
  my $storage_maker = shift;
  my $storage_type = shift;

  # yes, yes, this blows.
  my $make_ref = $storage_type =~ /memory/i ? sub {\$_[0]} : sub {[$_[0]]};

  my $continuum_spec = [
    ["server1", 10],
    ["server2", 1000],
    ["server3", 200],
  ];

  my $skv = make_skv($continuum_spec, $storage_maker);
  my @keys = (1..2000);
  $skv->set($_, $make_ref->("v$_")) for @keys;
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;

  # Setup new servers and an extended continuum
  $skv->storages->{"server$_"} = $storage_maker->() for 4..8;
  my $new_cont = $skv->continuum->clone;
  $new_cont->extend([
    ["server5", 120], ["server6", 1200],
    ["server7", 10], ["server8", 700],
  ]);
  isa_ok($new_cont, "ShardedKV::Continuum::Ketama");

  # set continuum
  $skv->begin_migration($new_cont);
  isa_ok($skv->migration_continuum, "ShardedKV::Continuum::Ketama");

  # Check that reads still work and return the old values
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;

  # Rewrite part of the keys
  my @first_half_keys = splice(@keys, 0, int(@keys/2));
  $skv->set($_, $make_ref->("N$_")) for @first_half_keys;

  # Check old and new keys
  is_deeply($skv->get($_), $make_ref->("v$_")) for @keys;
  is_deeply($skv->get($_), $make_ref->("N$_")) for @first_half_keys;

  if ($storage_type =~ /memory/i) {
    # FIXME support this part of the test for mysql!
    check_old_new("Many new servers", $skv, qr/^server[5-8]$/);
  }
}

# make sure that old values are on old servers, new values on either
# Breaking encapsulation of the in-Memory storage...
sub check_old_new {
  my ($name, $skv, $new_server_regex) = @_;

  # combined keys in new server
  my %new_exists;
  foreach my $sname (keys %{$skv->storages}) {
    my $server = $skv->storages->{$sname};
    my $hash = $server->hash;
    if ($sname =~ $new_server_regex) {
      $new_exists{$_} = undef for keys %$hash;
    }
  }

  foreach my $sname (keys %{$skv->storages}) {
    my $server = $skv->storages->{$sname};
    my $hash = $server->hash;
    if ($sname =~ $new_server_regex) {
      foreach (keys %$hash) {
        my $str = "$name: Old value 'v$_' for key '$_' in new server!";
        ok(${$hash->{$_}} =~ /^N/, $str);
      }
    }
    else {
      foreach (keys %$hash) {
        my $str = "$name: New value 'N$_' for key '$_' in old server as 'v$_'!";
        if (exists $new_exists{$_}) {
          ok(${$hash->{$_}} =~ /^v/, $str);
        }
      }
    }
  }
}

# make a new ShardedKV from a continuum spec
sub make_skv {
  my $cont_spec = shift;
  my $storage_maker = shift;
  my $continuum = ShardedKV::Continuum::Ketama->new(from => $cont_spec);

  my $skv = ShardedKV->new(
    storages => {},
    continuum => $continuum,
  );
  foreach (@$cont_spec) {
    $skv->storages->{$_->[0]} = $storage_maker->();
  }
  return $skv;
}

1;
