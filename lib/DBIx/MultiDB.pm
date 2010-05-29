package DBIx::MultiDB;

use warnings;
use strict;

use Carp;
use Data::Dumper;
use DBI;

our $VERSION = '0.01';

sub new {
    my ( $class, %param ) = @_;

    my $dbh = DBI->connect( $param{dsn} );

    bless { base => { %param, dbh => $dbh }, remote => [] }, $class;
}

sub attach {
    my ( $self, %param ) = @_;

    my $dbh = DBI->connect( $param{dsn} );

    push @{ $self->{remote} }, { %param, dbh => $dbh };
}

sub execute {
    my $self = shift;

    for my $remote ( @{ $self->{remote} } ) {
        my $data =
          $remote->{dbh}->selectall_hashref( $remote->{sql}, $remote->{key}, );

        $remote->{data} = $data;
    }

    my $dbh = $self->{base}->{dbh};
    my $sql = $self->{base}->{sql};

    my $sth = $dbh->prepare($sql);

    $sth->execute();

    $self->{base}->{sth} = $sth;

	return $self;
}

sub fetchrow_hashref {
    my $self = shift;

    my $sth = $self->{base}->{sth};

    my $row = $sth->fetchrow_hashref();

	my %row = %{ $row }; # copy

    for my $remote ( @{ $self->{remote} } ) {
		my $key   = $remote->{referenced_by};
		my $value = $row{$key};

		delete $row{$key}; # it will be replaced by the expanded data, which already includes an id

		my $prefix = $remote->{prefix} || '';
		for my $k ( keys %{ $remote->{data}->{$value} } ) {
			$row{"$prefix$k"} = $remote->{data}->{$value}->{$k};
		}
    }

    return \%row;
}

1;

__END__

=head1 NAME

DBIx::MultiDB - join data from multiple sources

=head1 SYNOPSIS

	use DBIx::MultiDB;
	
	my $query = DBIx::MultiDB->new(
	    dsn => 'dbi:SQLite:dbname=/tmp/db1.db',
	    sql => 'SELECT id, name, company_id FROM employee',
	);
	
	$query->attach(
	    prefix        => 'company_',
	    dsn           => 'dbi:SQLite:dbname=/tmp/db2.db',
	    sql           => 'SELECT id, name FROM company',
	    key           => 'id',
	    referenced_by => 'company_id',
	);
	
	$query->execute();

	while ( my $row = $query->fetchrow_hashref ) {
		# ...
	}

=head1 DESCRIPTION

DBIx::MultiDB provides a simple way to join data from different sources.

You are not limited to a single database engine: in fact, you can join data
from any source for which you have a DBI driver (MySQL, PostgreSQL, SQLite, 
etc). You can even mix them!

=head1 METHODS

=head2 new

Constructor. You must provide a dsn and sql, which is your base query.

=head2 attach

Once you have a base query, you can attach multiple queries that will be
joined to it. For each one, you must provide a dsn, sql, and the relationship
information (key and referenced_by). You can optionally provide a prefix
that will be used to prevent name clashes.

=head2 execute

Execute the query. (This will load all the attached queries, building an
index in memory.)

=head2 fetchrow_hashref

Return a hashref, containing field names and values.

=head1 AUTHOR

Nelson Ferraz, C<< <nferraz at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-multidb at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-MultiDB>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::MultiDB

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-MultiDB>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-MultiDB>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-MultiDB>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-MultiDB/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2010 Nelson Ferraz, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.
