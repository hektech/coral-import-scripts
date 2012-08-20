#!/usr/bin/perl
use strict;
use DBI;
use Text::CSV;
use Getopt::Long;

#################
##  DEV FLAGS  ##
#################
my $UPDATE_DB = 0; # 0 = don't make any changes to db
my $DEBUG = 0;


# GET COMMAND LINE OPTIONS
# the first few variables do not belong in columns hash, but I want GetOptions to save the column variables in the hash automatically; this was the simplest way I could find; for more info see:
# http://perldoc.perl.org/Getopt/Long.html#Storing-options-values-in-a-hash
my $help = '';
my $filename = '';
my $config_filename = 'coral_db.conf'; #set default
my $titlecase = '';
my $utf8 = '';
my %columns = ('help' => \$help, 'filename' => \$filename, 'config_file' => \$config_filename, 'titlecase' => \$titlecase, 'utf8' => \$utf8);
GetOptions (\%columns, 'help|h', 'filename|f=s', 'config_file=s', 'titlecase', 'utf8', 'title|t=s', 'title_num|t_num=s', 'issn=s', 'alt_issn=s', 'format=s', 'title_url|url=s', 'price=s', 'publisher|pub=s', 'publisher_number|pub_num=s', 'provider=s', 'platform=s', 'consortium=s', 'vendor=s');

my $missing = 0;
my @required_cols = ('filename', 'title', 'title_num', 'issn', 'publisher', 'publisher_number');
foreach (@required_cols) {
    $missing = 1 if not defined $columns{$_};
}

# If '-h' or '--help', or missing required options, print the help text
if ($help or $missing) {
    print <<"HELPTEXT";
    Usage: coral_generic_import.pl -f FILENAME COLUMNS
      COLUMNS: -title=N -title_num=N -issn=N -publisher=N -pub_num=N [-alt_issn=N -format=N -price=N -title_url=N -provider=N -platform=N -consortium=N -vendor=N -titlecase]

      -f [--filename]: File must be in CSV format
      -alt_issn: Provides a backup column if the first ISSN is blank (often used for print ISSN vs. e-ISSN)
      -titlecase: Capitalizes only the first letter of every word (very basic)

    NOTE: For any COLUMNS variable, give an integer to point to a column in the file (begins with zero); or give a value in quotes.
HELPTEXT

    if ($DEBUG) { #output variables
        print "Opening file: $filename...\n";
        foreach (@required_cols) {
            if ( !exists($columns{$_}) ) {
                print "ERROR: You must provide all of the following:\n\t" . join("\n\t", @required_cols) . "\n";
            }
            print "$_: $columns{$_}\n";
        }
    }
    exit;
}


if ($UPDATE_DB) {
    print "THIS WILL CHANGE THE DATABASE. PROCEED? [y/N] ";
    my $input = <STDIN>;
    chomp $input;
    if ($input !~ /^[yY]$/) {
        print "Aborting.\n";
        exit;
    }
} else {
    print "JUST TESTING (no changes to db; change UPDATE_DB to 1 to make real changes)...\n";
}


# OPEN CSV FILE
# (do this early; don't waste time with DB if file not found)
my $csv = Text::CSV->new({ binary => 1 }) or die "Cannot use CSV: ".Text::CSV->error_diag ();
my $encoding = ":encoding(utf8)" if $utf8;
open my $fh, "<$encoding", $filename or die "$filename: $!\n";



# READ IN CORAL DB VARIABLES
our ($host, $port, $res_db, $org_db, $user, $pw);
do $config_filename; #grab DB variables

# PERL DBI CONNECT
print "Connecting to database...\n" if $DEBUG;
my $res_dbh = DBI->connect("dbi:mysql:$res_db:$host:$port", $user, $pw) or die $DBI::errstr;
my $org_dbh = DBI->connect("dbi:mysql:$org_db:$host:$port", $user, $pw) or die $DBI::errstr;


# GLOBALS
my %orgs = ();
my %orgs_by_alias = ();

my $count_orgs_found = 0;
my $count_orgs_created = 0;
my $count_orgs_updated = 0;
my $count_new_orgs_matched = 0;
my $count_res_created = 0;
my $count_res_found = 0;
my $count_alt_issns = 0;
my $count_aliases_added = 0;
my $count_contacts_added = 0;

# CONSTANTS
my $STATUS_PROGRESS = 1;
my $RESOURCE_TYPE_PERIODICALS = 4;
my $ALIAS_TYPE_EBSCO = 4;

# HELPFUL HASHES
my %ORG_ROLES = ('consortium' => 1, 'library' => 2, 'platform' => 3, 'provider' => 4, 'publisher' => 5, 'vendor' => 6);
my @optional_org_types = ('provider', 'platform', 'consortium', 'vendor');
my %format_ids = ( #match EBSCO format strings to Coral format ids
    'Print + Electronic' => 1, 
    'Print + Online' => 1, 
    'Electronic' => 2, 
    'Online' => 2, 
    'Print' => 3, 
    'Other' => 4
);
my %acq_type_ids = ('Paid' => 1, 'Free' => 2, 'Trial' => 3);
my %note_types = ('Product Details' => 1, 'Acquisition Details' => 2, 'Access Details' => 3, 'General' => 4, 'Licensing Details' => 5, 'Initial Note' => 6);


# PREPARE REPEATED QUERIES (for efficiency)

my $query;

# find existing resource by ISSN or title
$query = "SELECT `resourceID` FROM `Resource` WHERE `isbnOrISSN` LIKE ?";
my $qh_get_res = $res_dbh->prepare($query);

# create new resource
$query = "INSERT INTO `Resource` (`createDate`, `createLoginID`, `titleText`, `isbnOrISSN`, `statusID`, `resourceTypeID`, `resourceFormatID`, `acquisitionTypeID`, `descriptionText`, `resourceURL`) VALUES (CURDATE(), 'system', ?, ?, $STATUS_PROGRESS, $RESOURCE_TYPE_PERIODICALS, ?, ?, ?, ?)";
my $qh_new_res = $res_dbh->prepare($query);

# TODO add a payment; which fundName to use? ex. "Periodicals AA 2012"
# INSERT INTO `ResourcePayment`(`resourceID`, `fundName`, `selectorLoginID`, `paymentAmount`, `orderTypeID`, `currencyCode`) VALUES ([value-1],[value-2],[value-3],[value-4],[value-5],[value-6])
# my %order_types = ('Ongoing' => 1, 'One Time' => 2);

# create new organization
$query = "INSERT INTO `Organization` (`createDate`, `createLoginID`, `name`, `noteText`) VALUES (CURDATE(), 'system', ?, ?)";
my $qh_new_org = $org_dbh->prepare($query);

# link resource to an organization (only for new orgs)
$query = "INSERT INTO `ResourceOrganizationLink` (`resourceID`, `organizationID`, `organizationRoleID`) VALUES (?, ?, ?)";
my $qh_res_org_link = $res_dbh->prepare($query);

# find existing links for this org and resource
$query = "SELECT `organizationRoleID` FROM `ResourceOrganizationLink` WHERE `resourceID` = ? AND `organizationID` = ?";
my $qh_find_res_org_links = $res_dbh->prepare($query);

# check for existing EBSCO alias for org
$query = "SELECT `name` FROM `Alias` WHERE `organizationID` = ? AND `aliasTypeID` = $ALIAS_TYPE_EBSCO";
my $qh_get_alias = $org_dbh->prepare($query);

# create ESBCO alias for an organization
$query = "INSERT INTO `Alias` (`organizationID`, `aliasTypeID`, `name`) VALUES (?, $ALIAS_TYPE_EBSCO, ?)";
my $qh_new_alias = $org_dbh->prepare($query);

# add contact info for an organization
# TODO OR just make a scraper script to grab info from EBSCOnet
#$query = "INSERT INTO `Contact` (`organizationID`, `lastUpdateDate`, `addressText`, `noteText`) VALUES (?, NOW(), ?, 'IMPORTED FROM EBSCO')";
#my $qh_new_contact = $org_dbh->prepare($query);

# TODO add role for new Org contact (Role: 3 => Sales)
#$query = "INSERT INTO `ContactRoleProfile`(`contactID`, `contactRoleID`) VALUES (?, 3)";
#my $qh_new_contact_role = $org_dbh->prepare($query);



# GRAB EXISTING ORGS FROM CORAL DB
$query = "SELECT `organizationID`, `name` FROM `Organization`";
my $qh_temp = $org_dbh->prepare($query);
$qh_temp->execute();

while (my ($org_id, $org_name) = $qh_temp->fetchrow_array) {
    # check for EBSCO alias
    $qh_get_alias->execute($org_id);
    my $alias_name = $qh_get_alias->fetchrow_array; #name or undef
    my $clean_name = standardize_org_name($org_name);
    $orgs{$clean_name} = {
        'id' => $org_id, 
        'matches' => 0, 
        'ebsco_alias' => $alias_name, #filled when alias is found or created
        'coral_name' => $org_name,
    };
    #use the alias if has one, since alias should not change
    if ($alias_name) {
        $orgs_by_alias{$alias_name} = $orgs{$clean_name};
    }
}


# READ LINES FROM FILEHANDLE
print "Reading file...\n" if $DEBUG;
my $line_num = 0;
my $row = $csv->getline( $fh ); # THROW AWAY HEADER ROW

while ( $row = $csv->getline( $fh ) ) {
    $line_num++;

    my %values = ();
    foreach my $key (keys %columns) {
        #if column reference, grab value from row
        if ($columns{$key} =~ /^(\d+)/) {
            $values{$key} = $row->[$1];
        } else { #otherwise, grab constant
            $values{$key} = $columns{$key};
        }
    }

    my $pub_name = $values{'publisher'};
    my $pub_addr = $values{'publisher_address'}; #TODO add as a Contact
    my $pub_num = $values{'publisher_number'};

# CREATE ORGANIZATION
    my %org_ids = ();
    $org_ids{'publisher'} = create_org($pub_name, $pub_addr, $pub_num);

# CREATE RESOURCE
    #grab org id for optional orgs
    foreach my $org_type (@optional_org_types) {
        if (defined($columns{$org_type})) {
            my $temp_name = standardize_org_name($values{$org_type});
            if (exists $orgs{$temp_name}) {
                $org_ids{$org_type} = $orgs{$temp_name}->{id};
            } else {
                print "WARN: Org not found: $org_type = [$temp_name]\n";
            }
        }
    }
    create_res(\%org_ids, \%values);
}

$csv->eof or $csv->error_diag();
close $fh;


# PRINT ORGS: SHOWS WHICH EBSCO ORGS WERE FOUND IN CORAL, EBSCO OR BOTH
if ($DEBUG) {
    foreach my $org_name (sort keys %orgs) {
        my $ebsco_name = $orgs{$org_name}->{'ebsco_alias'};
        my $coral_name = $orgs{$org_name}->{'coral_name'};

        if (defined $orgs{$org_name}->{'matches'}) {
            if ($orgs{$org_name}->{'matches'} > 0) {
                print "BOTH : $ebsco_name\n\t$coral_name (". $orgs{$org_name}->{'matches'} .")\n";
            } else {
                print "CORAL: $org_name ($coral_name)\n";
            }
        } else {
            print "EBSCO: $org_name ($ebsco_name)\n";
        }
    }
}

# OUTPUT SUMMARY
print "\n---------------------------------------\n";
print "Found: $count_orgs_found Orgs (already in Coral)\n";
print "Created: $count_orgs_created Orgs\n";
print "- matched: $count_new_orgs_matched Orgs\n";
print "Added: $count_aliases_added Org Aliases\n";
#print "Added: $count_contacts_added Org Contacts\n";
print "Found: $count_res_found Resources (already in Coral)\n";
print "Created: $count_res_created Resources\n";
print "- used $count_alt_issns Alternate ISSNs\n";
print "---------------------------------------\n";

if (!$UPDATE_DB) {
    print "*** JUST TESTING (no changes to db) ***\n";
    print "*** - change UPDATE_DB to 1 to make ***\n";
    print "***   real changes.                 ***\n";
    print "---------------------------------------\n";
}


exit;




# FUNCTION: Create or SKIP an Organization (searching by name)
# - don't overwrite any data
# - add an alias for EBSCO version of Org name
# - add link to EBSCOnet record
sub create_org {
    my ($org_ebsco, $org_addr, $org_num) = (shift, shift, shift);
    my $org_clean = standardize_org_name($org_ebsco);
    my $note_text = "<a href='http://www.ebsconet.com/publisher.aspx?PublisherNumber=$org_num'>EBSCO link</a>";
    my $org;

    if (exists $orgs_by_alias{$org_ebsco}) {
        $org = $orgs_by_alias{$org_ebsco};
    } elsif(exists $orgs{$org_clean}) {
        $org = $orgs{$org_clean};
    }

    if ($org) {
        if (defined $org->{'coral_name'}) { # must be in coral
            $count_orgs_found++;
            $org->{'matches'}++;
        } else {
            $count_new_orgs_matched++;
        }
    } else {
        # create blank entry in hash, to be filled later
        $org = {'id' => undef, 'matches' => undef, 'ebsco_alias' => undef, 'coral_name' => undef};

        # create new Org in Coral
        my $org_ebsco_clean = lc($org_ebsco);
        $org_ebsco_clean =~ s/%.*$//; #remove "%" and everything after
        $org_ebsco_clean =~ s/\b(\w)/\u$1/g; #capitalize first letter of each word
        my $rows_affected = $qh_new_org->execute($org_ebsco_clean, $note_text) if $UPDATE_DB;

        if ($rows_affected == 1 or !$UPDATE_DB) {
            $org->{'id'} = $org_dbh->last_insert_id(0, 0, 0, 0); #0s prevent error about expected params, but mysql appears to ignore them (re: DBI docs in cpan)
            $count_orgs_created++;
        } else {
            warn "QUERY ERROR: Cannot create Org: $org_ebsco\n";
        }
        $orgs_by_alias{$org_ebsco} = $org;
    }

    my $org_id = $org->{'id'};
    if (($org_id > 0 or !$UPDATE_DB) and !$org->{'ebsco_alias'}) {
        my $rows_affected = $qh_new_alias->execute($org_id, $org_ebsco) if $UPDATE_DB;
        if ($rows_affected == 1 or !$UPDATE_DB) {
            $org->{'ebsco_alias'} = $org_ebsco;
            $count_aliases_added++;
        } else {
            warn "QUERY ERROR: Cannot create Alias for Org ID: $org_id, Name: $org_ebsco\n";
        }
    }
    #add contact info for this org
#    $rows_affected = $qh_new_contact->execute($org_id, $org_addr) if $UPDATE_DB;
#    if ($rows_affected == 1 or !$UPDATE_DB) {
#        $count_contacts_added++;
#        my $contact_id = $org_dbh->last_insert_id(0, 0, 0, 0); #0s prevent error about expected params, but mysql appears to ignore them (re: DBI docs in cpan)
#        $qh_new_contact_role->execute($contact_id) if $UPDATE_DB;
#    } else {
#        print "QUERY ERROR: Cannot add Contact to Org ID: $org_id, Address: $org_addr\n";
#    }

    return $org_id;
}


# FUNCTION: Create or SKIP a resource if found (matching on ISSN)
# - don't overwrite any data
# - add link to EBSCOnet record, if needed
sub create_res {
    my ($org_ids_ref, $params) = (shift, shift);

    my $title = $params->{'title'};
    my $title_num = $params->{'title_num'};
    my $issn = $params->{'issn'};
    my $format_id = undef;
    if (defined $params->{'format_id'}) {
        $format_id = $format_ids{$params->{'format_id'}} ? $format_ids{$params->{'format_id'}} : $format_ids{'Other'};
    }
    my $acq_type_id = undef;
    if (defined $params->{'price'}) {
        my $price = $params->{'price'};
        $acq_type_id = ($price > 0) ? $acq_type_ids{'Paid'} : $acq_type_ids{'Free'}; #if 'Amount' > 0
    }
    my $url = $params->{'title_url'};
    my $res_id = 0;

    # if ISSN column is blank, use alternate if supplied
    if (!$issn and defined($params->{'alt_issn'})) {
        $issn = $params->{'alt_issn'};
        $count_alt_issns++;
    }
    my ($checkable_issn, $standardized_issn) = ($issn, $issn);
    $checkable_issn =~ s/[- ]/_/g; #for searching DB, wildcard allows for various ISSN formats
    $standardized_issn =~ s/[- ]/-/g; #for inserting into DB

    if ($url !~ m{http://.*}) {
        $url = '';
    }

# SEARCH BY ISSN
    if ($standardized_issn =~ /[xX\d]{4}-[xX\d]{4}/) {
        $qh_get_res->execute($checkable_issn); #only returns true, not num of rows
        $res_id = $qh_get_res->fetchrow_array; #grab the ID

        if ($res_id) { #if found a match
            print "**** SKIPPING Existing Resource ($res_id): [$issn] $title ($url)\n";
            $count_res_found++;
        }
    } else {
        print "#### MAY CREATE DUPLICATE (bad or missing ISSN [$standardized_issn])...\n";
        $standardized_issn = "";
    }

    if (!$standardized_issn or !$res_id) {
        $title = cleanup_res_name($title);
        if ($titlecase) {
            $title = lc($title);
            $title =~ s/\b(\w)/\u$1/g; #capitalize first letter of each word
        }
        print "Creating new Resource: [$standardized_issn] $title ($url)\n";

        # undef/null values are usually allowed
        my $rows_affected = $qh_new_res->execute($title, $standardized_issn, $format_id, $acq_type_id, "<a href='http://www.ebsconet.com/titledetail.aspx?TitleNumber=$title_num'>EBSCO link</a>", $url) if $UPDATE_DB;
        if ($rows_affected == 1 or !$UPDATE_DB) {
            $count_res_created++;
            $res_id = $res_dbh->last_insert_id(0, 0, 0, 0); #0s prevent error about expected params, but mysql appears to ignore them (re: DBI docs in cpan)

            # linking resource to publisher
            $qh_res_org_link->execute($res_id, $org_ids_ref->{'publisher'}, $ORG_ROLES{'publisher'}) if $UPDATE_DB;
            # linking resource to optional orgs
            foreach my $org_type (@optional_org_types) {
                if (defined $org_ids_ref->{$org_type}) {
                    $qh_res_org_link->execute($res_id, $org_ids_ref->{$org_type}, $ORG_ROLES{$org_type}) if $UPDATE_DB;
                }
            }
        }
    }
    return $res_id;
}


# FUNCTION: Clean up and standardize the Org name
# This is needed because different companies list the same publisher under slightly different names
sub standardize_org_name {
    my $name = shift;
    $name = uc($name);

    # SINGLE CASES: replace EBSCO name with CORAL name, before other changes
    $name =~ s/^AMER PHYSIOL SOC% LUCIA TAYIEL$/AMERICAN PHYSIOLOGICAL SOCIETY/i;
    $name =~ s/^ANTIQUITY PUBLICATIONS LIMITED$/ANTIQUITY/i;
    $name =~ s/^ARCHAEOLOGY INST OF AMERICA$/ARCHAEOLOGICAL INSTITUTE OF AMERICA/i;
    $name =~ s/^COLLEGE ART ASSN OF AMERICA$/COLLEGE ART ASSOCIATION/i;
    $name =~ s/^CONSEJO SUPERIOR INVESTIGACION$/CONSEJO SUPERIOR DE INVESTIGACIONES CIENTIFICAS/i;
    $name =~ s/^DE GRUYTER$/WALTER DE GRUYTER/i;
    $name =~ s/^H W WILSON CO$/HW WILSON COMPANY/i;
    $name =~ s/^LIBRAIRIE DROZ SA$/LIBRAIRIE DROZ/i;
    $name =~ s/^MARINE BIOLOGICAL LABORATORIES$/MARINE BIOLOGICAL LABORATORY/i;
    $name =~ s/^MATHEMATICAL SCIENCE PUBL$/MATHEMATICAL SCIENCES/i;
    $name =~ s/^MODERN LANGUAGE ASSN OF AMER$/MODERN LANGUAGE ASSOCIATION/i;
    $name =~ s/^SOC INDUST APPLIED MATHEMATICS$/SOCIETY INDUSTRIAL APPLIED MATHEMATICS/i;
    $name =~ s/^TAYLOR & FRANCIS GROUP$/TAYLOR FRANCIS/i;
    $name =~ s/^UNIV OF MICHIGAN \/DEPT OF MATH$/UNIVERSITY MICHIGAN DEPARTMENT MATHEMATICS/i;

    # REMOVE punctuation, common words, abbrevs, extra spaces
    $name =~ s/[&,'\.]//g;
    $name =~ s/-/ /g;
    $name =~ s/\b(CO|INC|LLC|LTD|GMBH|AND|FOR|OF)\b//g; #often omitted
    $name =~ s/\b(PUBL|PUBLISHERS|PUBLISHING)\b//g; #often abbrev, generic
    $name =~ s/\b(LIMITED|PRESS)\b//g; #often abbrev, generic
#creates false positivies # $name =~ s/\/.*$//; #remove "/" and everything after 
    $name =~ s/%.*$//; #remove "%" and everything after
    $name =~ s/^[\s]*//; #trim opening whitespace
    $name =~ s/[\s]*$//; #trim trailing whitespace
    $name =~ s/[\s][\s]+/ /g; #collapse whitespace into one space

    # EXPAND certain abbrevs
    $name =~ s/\bAERO\b/AERONAUTICS/i;
    $name =~ s/\bAMER\b/AMERICAN/i; #or America?
    $name =~ s/\bASSN\b/ASSOCIATION/i;
    $name =~ s/\b(CTR|CNTR)\b/CENTER/i;
    $name =~ s/\bINST\b/INSTITUTE/i;
    $name =~ s/\bNATL\b/NATIONAL/i;
    $name =~ s/\bNO\b/NORTH/i;
    $name =~ s/\b(RES|RSCH)\b/RESEARCH/i;
    $name =~ s/\bSOC\b/SOCIETY/i;
    $name =~ s/\bSVCS\b/SERVICES/i;
    $name =~ s/\bUNIV\b/UNIVERSITY/i;
    
    return $name;
}


# FUNCTION: Clean up and standardize the Resource name
sub cleanup_res_name {
    my $name = shift;
    $name = uc($name);

    # REMOVE extra spaces
    $name =~ s/^[\s]*//; #trim opening whitespace
    $name =~ s/[\s]*$//; #trim trailing whitespace
    $name =~ s/[\s][\s]+/ /g; #collapse whitespace into one space

    return $name;
}

