#!/usr/bin/perl
use strict;
use warnings;

# vim:noet:ts=4:sw=4:

# Our bibids are placed in subfield 035a
# Our holdings are stored in field 852 with the following structure (roughly):
# a - call number



use MARC::Batch;
use MARC::File::XML (BinaryEncoding => "utf8", RecordFormat => "USMARC");
use POSIX qw( strftime );

my $holdings_file = 'mdata0.xml';
my $sql_file = 'insert_holdings.sql';

# We'll dump our holdings entries in this list
my @holdings;

my $batch = MARC::Batch->new('XML', $holdings_file);

parse_holdings($batch, \@holdings);
format_holdings_for_insert(\@holdings);

sub parse_holdings {
	my $batch = shift;
	my $holdings = shift;
	my $record;
	while ($record = $batch->next()) {
		my $bibid = $record->subfield('035', 'a');
		$bibid =~ s/\D+//gso;
		my $createdate =  strftime("%Y%m%d", localtime);
		my @copies = $record->field('852');
		foreach my $copy (@copies) {
			my %copy_attributes;
			$copy_attributes{'bibid'} = $bibid;
			$copy_attributes{'callnum'} = escape_quotes($copy->subfield('k')) .  escape_quotes($copy->subfield('h')) .  escape_quotes($copy->subfield('i'));
			$copy_attributes{'barcode'} = escape_quotes($copy->subfield('p'));
			$copy_attributes{'location'} = escape_quotes($copy->subfield('b'));
			$copy_attributes{'library'} = escape_quotes($copy->subfield('a'));
			$copy_attributes{'item_type'} = escape_quotes($copy->subfield('x'));
			$copy_attributes{'createdate'} = $createdate;
			push(@$holdings, \%copy_attributes);
		}
	}
}

sub format_holdings_for_insert {
	my $holdings = shift;

	open(SQLFH, '>', $sql_file) or die $!;
	binmode(SQLFH, ":utf8");

# We could do the inserts inside a transaction in case of problems

# Right now, the data contains an invalid date format (DD/M/YYYY)
# that PostgreSQL doesn't like (it wants YYYY-MM-DD), so we'll go
# ahead and just commit each row if possible

#	print SQLFH "BEGIN;\n\n";

	foreach my $holding (@$holdings) {
		printf SQLFH "INSERT INTO staging_items (bibkey, callnum, createdate, location, barcode, item_type, owning_lib) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n", 
		       $holding->{'bibid'},
		       $holding->{'callnum'},
		       $holding->{'createdate'},
		       $holding->{'location'},
		       $holding->{'barcode'},
		       $holding->{'item_type'},
		       $holding->{'library'}
		;
	}

#	print SQLFH "\nCOMMIT;";

	close SQLFH;
}

sub escape_quotes {
	my $string = shift;
	$string =~ s/'/''/g;
	return $string;
}
