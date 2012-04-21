package ShardedKV::Storage::MySQL::ActiveKeyMigration;
use strict;
use warnings;
use Scalar::Util qw(blessed);
use Carp qw(croak);
use Time::HiRes qw(sleep);

# WARNING: Consider this untested alpha code. Use at your own risk!


# MySQL storage only!
sub migrate_to_additional_storage {
  my %args = @_;

  my $skv = $args{shardedkv};
  if (not blessed($skv) or not $skv->isa("ShardedKV")) {
    croak("Need ShardedKV object as first parameter");
  }
  my $chunksize = $args{chunksize} || 1000;
  my $chunksleep = defined($args{chunksleep}) ? $args{chunksleep} : 1;

  my $storages = $skv->storages;

  my $orig_continuum = $skv->continuum;
  my $migr_continuum = $skv->migration_continuum;
  my @orig_buckets = $orig_continuum->get_bucket_names;
  my @migr_buckets = $migr_continuum->get_bucket_names;

  my %new_buckets = map {$_ => 1} @migr_buckets;
  delete $new_buckets{$_} for @orig_buckets;

  die "No new storages?"
    if not keys %new_buckets;

  my %dest_dbhs = map {$_ => $storages->{$_}->mysql_connection} keys %new_buckets;
  my %new_insert_fragments;
  foreach my $bname (keys %dest_dbhs) {
    my $storage = $storages->{$bname};
    my ($dest_tbl, $dest_key, $dest_val) = map $storage->$_(),
                                        qw(table_name key_col_name value_col_name);
    my $insert_frag = qq{
      INSERT IGNORE INTO $dest_tbl
      ($dest_key, $dest_val)
      VALUES
    };
    $new_insert_fragments{$bname} = $insert_frag;
  }

  foreach my $storage_name (@orig_buckets) {
    warn "Fetching from '$storage_name'";
    my $src_storage = $storages->{$storage_name}
      or die "Invalid bucket name '$storage_name'";
    my $src_dbh = $src_storage->mysql_connection;
    my ($src_tbl, $src_key, $src_val) = map $src_storage->$_(),
                                        qw(table_name key_col_name value_col_name);

    my $src_prep_first = $src_dbh->prepare(qq{
      SELECT $src_key, $src_val FROM $src_tbl
      ORDER BY $src_key LIMIT $chunksize
    });
    my $src_prep = $src_dbh->prepare(qq{
      SELECT $src_key, $src_val FROM $src_tbl
      WHERE $src_key > ? ORDER BY $src_key LIMIT $chunksize
    });

    my $del_fragment = qq{
      DELETE FROM $src_tbl WHERE $src_key IN
    };

    my $cur_key = $src_dbh->selectcol_arrayref("SELECT MIN($src_key) FROM $src_tbl")->[0];

    my $first = 1;
    while (1) {
      my $sth;
      if ($first) {
        $sth = $src_prep_first;
        $sth->execute();
        $first = 0;
      }
      else {
        $sth = $src_prep;
        $sth->execute($cur_key);
      }
      my $rows = $sth->fetchall_arrayref;
      last if not @$rows;
      $cur_key = $rows->[-1][0];

      my %new_storage_rows;
      my @to_delete;
      foreach my $row (@$rows) {
        my $sname = $migr_continuum->choose($row->[0]);
        if (exists $new_buckets{$sname}) {
          push(@to_delete, $row->[0]);
          push(@{$new_storage_rows{$sname}||=[]}, $row);
        }
      }

      if (@to_delete) {
        foreach my $new_sname (keys %new_storage_rows) {
          my $insert_sql = $new_insert_fragments{$new_sname};
          my $insert_data = $new_storage_rows{$new_sname};
          my $qstr = join ',', ( ("(?,?)") x scalar(@$insert_data) );
          $dest_dbhs{$new_sname}->do(qq{$insert_sql $qstr}, undef, (map @$_, @$insert_data))
            or die "Failed to insert migrated keys into new storage";
        }

        # remove from old storage
        my $nto_remove = @to_delete;
        my $qstr = join ',', ( ('?') x $nto_remove );
        $src_dbh->do(qq{$del_fragment ($qstr)}, undef, @to_delete)
          or die "Failed to remove migrated keys from old storage";
      }

      warn "Sleeping before next chunk...";
      sleep($chunksleep);
    }
  } # end foreach old/src storage
}

1;
