#!/usr/bin/perl 

use strict;
use warnings;

use FindBin qw($Bin);

system("sh $Bin/init_db.sh");

use Test::More;
use Test::Deep;
use Data::Dumper;

use DBIx::MultiDB;

my $query = DBIx::MultiDB->new(
    dsn => 'dbi:SQLite:dbname=/tmp/db1.db',
    sql => 'SELECT id, name, company_id FROM employee',
);

$query->left_join(
    dsn           => 'dbi:SQLite:dbname=/tmp/db2.db',
    sql           => 'SELECT id AS company_id, name AS company_name FROM company',
    key           => 'company_id',  # in this query
    referenced_by => 'company_id',  # in base query
);

$query->execute();

my $r = $query->fetchrow_hashref;

my $expected_result = {
    'company_id'   => '1',
    'company_name' => 'a',
    'name'         => 'a1',
    'id'           => '1'
};

plan tests => 1;

cmp_deeply( $r, $expected_result )
  or print Dumper $r;
