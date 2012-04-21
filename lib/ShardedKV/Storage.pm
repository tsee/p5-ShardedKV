package ShardedKV::Storage;
use Moose::Role;

requires qw(get set delete);

no Moose;

1;

__END__

=head1 NAME

ShardedKV::Storage - ...

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SEE ALSO

L<ShardedKV>

=head1 AUTHOR

Steffen Mueller E<lt>smueller@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Steffen Mueller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
