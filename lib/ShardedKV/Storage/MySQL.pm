package ShardedKV::Storage::MySQL;
use Moose;
# ABSTRACT: MySQL storage backend for ShardedKV

with 'ShardedKV::Storage';

has 'mysql_master_connector' => (
  is => 'rw',
  isa => 'CodeRef',
  required => 1,
);

# This could be shared among many "::Storage::MySQL" objects since we're
# single-threaded (and they would not work across multiple ithreads anyway).
# All that fancy logic would be done by the user-supplied connector code ref
# above which needs to know how to obtain a new or existing connection.
# This means that we can make each Storage::MySQL object be specific to
# a particular table!
has 'mysql_connection' => (
  is => 'rw',
  lazy => 1,
  builder => '_make_master_conn',
);

sub _make_master_conn {
  my $self = shift;
  return $self->mysql_master_connector->();
}

has 'table_name' => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

has 'key_col_name' => (
  is => 'ro',
  default => "keystr",
);
has 'key_col_type' => (
  is => 'ro',
  default => "CHAR(16) NOT NULL",
);

#has 'timestamp_col_name' => (
#  is => 'ro',
#  default => "last_updated",
#);
#has 'timestamp_col_type' => (
#  is => 'ro',
#  default => "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
#);

has 'value_col_name' => (
  is => 'ro',
  default => 'val',
);
has 'value_col_type' => (
  is => 'ro',
  default => 'MEDIUMBLOB NOT NULL',
);

# Could be prepared, but that is kind of nasty wrt. reconnects, so let's not go
# there unless we have to!
has 'get_query' => (
  is => 'ro',
  isa => 'Str',
  lazy => 1,
  builder => '_make_get_query',
);
has 'set_query' => (
  is => 'ro',
  isa => 'Str',
  lazy => 1,
  builder => '_make_set_query',
);
has 'delete_query' => (
  is => 'ro',
  isa => 'Str',
  lazy => 1,
  builder => '_make_delete_query',
);

sub _make_get_query {
  my $self = shift;
  my $tbl = $self->table_name;
  my ($key_col, $v_col) = map $self->$_, qw(key_col_name value_col_name);
  return qq{SELECT $v_col FROM $tbl WHERE $key_col = ? LIMIT 1};
}
sub _make_set_query {
  my $self = shift;
  my $tbl = $self->table_name;
  my ($key_col, $v_col) = map $self->$_, qw(key_col_name value_col_name);
  return qq{
    INSERT INTO $tbl ($key_col, $v_col) VALUES (?, ?)
    ON DUPLICATE KEY UPDATE
    $v_col = VALUES($v_col)
  };
}
sub _make_delete_query {
  my $self = shift;
  my $tbl = $self->table_name;
  my $key_col = $self->key_col_name;
  return qq{DELETE FROM $tbl WHERE $key_col = ? LIMIT 1};
}

sub prepare_table {
  my $self = shift;
  my $tbl = $self->table_name;
  my ($key_col, $key_type, $v_col, $v_type)
    = map $self->$_, qw(key_col_name key_col_type value_col_name value_col_type);
  $self->get_master_dbh->do(
    qq{
      CREATE TABLE IF NOT EXISTS $tbl (
        $key_col $key_type,
        $v_col $v_type,
        PRIMARY KEY($key_col)
      ) ENGINE=InnoDb
    },
    undef,
  );
}

# Might not reconnect if the mysql_master_connector code ref just returns
# a cached connection.
sub refresh_connection {
  my $self = shift;
  delete $self->{mysql_connection};
  return $self->mysql_connection;
}

sub get_master_dbh {
  my $self = shift;
  # fetch from master by default (TODO revisit later)
  my $master_dbh = $self->mysql_connection;
  if (not defined $master_dbh) {
    $master_dbh = $self->refresh_connection;
  }
  if (not defined $master_dbh) {
    die "Failed to get connection to mysql!";
  }
  return $master_dbh;
}

sub get {
  my ($self, $key) = @_;

  my $rv = $self->get_master_dbh->selectall_arrayref($self->get_query, undef, $key);
  return ref($rv) ? $rv->[0] : undef;
}

sub set {
  my ($self, $key, $value_ref) = @_;

  my $rv = $self->get_master_dbh->do($self->set_query, undef, $key, @$value_ref);
  return $rv ? 1 : 0;
}

sub delete {
  my ($self, $key) = @_;

  my $rv = $self->get_master_dbh->do($self->delete_query, undef, $key);
  return $rv ? 1 : 0;
}

no Moose;
__PACKAGE__->meta->make_immutable;

__END__

=head1 SYNOPSIS

  TODO

=head1 DESCRIPTION

A C<ShardedKV> storage backend that C<DBI> and C<DBD::mysql> to
store data in a MySQL table.

Implements the C<ShardedKV::Storage> role.

TODO more docs

=head1 SEE ALSO

=for :list
* L<ShardedKV>
* L<ShardedKV::Storage>
* L<DBI>
* L<DBD::mysql>

=cut
