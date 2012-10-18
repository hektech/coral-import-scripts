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
my $help;
my $filename;
my $config_filename = 'coral_db.conf'; #set default
my $titlecase;
my $utf8;
my %columns = ('help' => \$help, 'filename' => \$filename, 'config_file' => \$config_filename, 'titlecase' => \$titlecase, 'utf8' => \$utf8);
GetOptions (\%columns, 'help|h', 'filename|f=s', 'config_file=s', 'titlecase|tc', 'utf8', 'title|t=s', 'price=s', 'fund=s', 'order_type=s', 'purchasing_site|purch_site|site=s', 'issn=s', 'alt_issn=s', 'url=s', 'publisher=s', 'provider=s', 'platform=s', 'consortium=s', 'vendor=s');

my $missing = 0;
my @required_cols = ('filename', 'title', 'issn', 'url', 'publisher');
foreach (@required_cols) {
    $missing = 1 if not defined $columns{$_};
}

# If '-h' or '--help', or missing required options, print the help text
if ($help or $missing) {
    print <<"HELPTEXT";
Usage: coral_generic_import.pl -f FILENAME COLUMNS

      COLUMNS: -title=N -title_num=N -issn=N -publisher=N -pub_num=N [-alt_issn=N -format=N -price=N -fund=N -order_type=N -purchasing_site=N -title_url=N -provider=N -platform=N -consortium=N -vendor=N -titlecase -utf8]

      -f [--filename]:  File must be in CSV format
      -alt_issn:        Specify a backup column if the first ISSN is blank (often used for print ISSN vs. e-ISSN)
      -titlecase:       Capitalize only the first letter of every word in title (very basic)
      -utf8:            Read the CSV file as UTF-8 data

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

# CONNECT TO CORAL DB SERVER
my $res_dbh = DBI->connect("dbi:mysql:$res_db:$host:$port", $user, $pw) or die $DBI::errstr;
my $org_dbh = DBI->connect("dbi:mysql:$org_db:$host:$port", $user, $pw) or die $DBI::errstr;


# GLOBALS
my %orgs = ();

my $count_res_created = 0;
my $count_res_found = 0;
my $count_alt_issns = 0;
my $count_pymt_added = 0;
my $count_orgs_found = 0;
my $count_orgs_created = 0;
my $count_new_orgs_matched = 0;

# CONSTANTS
my $STATUS_IN_PROGRESS = 1;
my $RESOURCE_TYPE_PERIODICALS = 4;
my $CURRENCY_CODE_USA = 'USD';

# HELPFUL HASHES
my %ORG_ROLES = ('consortium' => 1, 'library' => 2, 'platform' => 3, 'provider' => 4, 'publisher' => 5, 'vendor' => 6);
my @optional_org_types = ('provider', 'platform', 'consortium', 'vendor');
my %acq_type_ids = ('Paid' => 1, 'Free' => 2, 'Trial' => 3);
my %order_types = ('Ongoing' => 1, 'One Time' => 2);
my %purchase_sites = ('Main Library' => 1, 'Seminary' => 2); #DB table name: PurchaseSite


# PREPARE REPEATED QUERIES (for efficiency)
my $query;

# find existing resource by ISSN or title
$query = "SELECT `resourceID` FROM `Resource` WHERE `isbnOrISSN` LIKE ?";
my $qh_get_res = $res_dbh->prepare($query);

# create new resource
$query = "INSERT INTO `Resource` (`createDate`, `createLoginID`, `titleText`, `isbnOrISSN`, `statusID`, `resourceTypeID`, `resourceURL`) VALUES (CURDATE(), 'system', ?, ?, $STATUS_IN_PROGRESS, $RESOURCE_TYPE_PERIODICALS, ?)";
my $qh_new_res = $res_dbh->prepare($query);

# create a payment for a resource
# NOTE: paymentAmount is in cents (i.e. $40.00 -> "4000")
$query = "INSERT INTO `ResourcePayment`(`resourceID`, `fundName`, `paymentAmount`, `orderTypeID`, `currencyCode`) VALUES (?, ?, ?, ?, '$CURRENCY_CODE_USA')";
my $qh_new_res_pymt = $res_dbh->prepare($query);

# create a purchasing site for a resource
$query = "INSERT INTO `ResourcePurchaseSiteLink`(`resourceID`, `purchaseSiteID`) VALUES (?, ?)";
my $qh_new_res_purch = $res_dbh->prepare($query);

# create new organization
$query = "INSERT INTO `Organization` (`createDate`, `createLoginID`, `name`) VALUES (CURDATE(), 'system', ?)";
my $qh_new_org = $org_dbh->prepare($query);

# link resource to an organization (only for new orgs)
$query = "INSERT INTO `ResourceOrganizationLink` (`resourceID`, `organizationID`, `organizationRoleID`) VALUES (?, ?, ?)";
my $qh_res_org_link = $res_dbh->prepare($query);

# find existing links for this org and resource
#$query = "SELECT `organizationRoleID` FROM `ResourceOrganizationLink` WHERE `resourceID` = ? AND `organizationID` = ?";
#my $qh_find_res_org_links = $res_dbh->prepare($query);


# GRAB EXISTING ORGS FROM CORAL DB
$query = "SELECT `organizationID`, `name` FROM `Organization`";
my $qh_temp = $org_dbh->prepare($query);
$qh_temp->execute();

while (my ($org_id, $org_name) = $qh_temp->fetchrow_array) {
    my $standard_name = standardize_org_name($org_name);
    $orgs{$standard_name} = {
        'id' => $org_id,
        'matches' => 0,
        #'imported_name' => '', #filled when Orgs are read from import file
        'coral_name' => $org_name,
    };
}


# READ LINES FROM FILEHANDLE
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

# CREATE ORGANIZATION
    my %org_ids = ();
    $org_ids{'publisher'} = create_org($pub_name);

# CREATE RESOURCE

    #grab org id for publisher, required
    my %org_ids = ();
    my $temp_name = standardize_org_name($values{'publisher'});
    if (exists $orgs{$temp_name}) {
        $org_ids{'publisher'} = $orgs{$temp_name}->{id};

        #grab org id for optional orgs
        foreach my $org_type (@optional_org_types) {
            if (defined($columns{$org_type})) {
                $temp_name = standardize_org_name($values{$org_type});
                if (exists $orgs{$temp_name}) {
                    $org_ids{$org_type} = $orgs{$temp_name}->{id};
                } else {
                    print "WARN: Org not found: $org_type = [$temp_name]\n";
                }
            }
        }
        create_res(\%org_ids, \%values);

    } else {
        #TODO consider creating the Org
        print "WARN: Could not create Resource without Org. Org not found: [$temp_name]\n";
    }
}

$csv->eof or $csv->error_diag();
close $fh;


# PRINT ORGS: SHOWS WHICH IMPORTED ORGS WERE FOUND IN CORAL ONLY, IMPORT FILE ONLY, OR BOTH
if ($DEBUG) {
    print "\n---------------------------------------\n\n";
    foreach my $org_name (sort keys %orgs) {
        my $imported_name = $orgs{$org_name}->{'imported_name'};
        my $coral_name = $orgs{$org_name}->{'coral_name'};

        if (defined $orgs{$org_name}->{'matches'}) {
            if ($orgs{$org_name}->{'matches'} > 0) {
                print "BOTH: $imported_name\n\t$coral_name (". $orgs{$org_name}->{'matches'} .")\n";
            } else {
                print "CORAL: $org_name ($coral_name)\n";
            }
        } else {
            print "IMPORT: $org_name ($imported_name)\n";
        }
    }
}

# OUTPUT SUMMARY
print "\n---------------------------------------\n";
print "Found: $count_orgs_found Orgs (already in Coral)\n";
print "Created: $count_orgs_created Orgs\n";
print "- matched: $count_new_orgs_matched Orgs\n";
print "Found  : $count_res_found Resources (already in Coral)\n";
print "Created: $count_res_created Resources\n";
print "-- used: $count_alt_issns Alternate ISSNs\n";
print "-- added: $count_pymt_added Prices\n";
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
sub create_org {
    my $org_input_name = shift;
    my $org_standard_name = standardize_org_name($org_input_name);
    my $org;

    if (exists $orgs{$org_standard_name}) {
        $org = $orgs{$org_standard_name};
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
        $org = {'id' => undef, 'matches' => undef, 'imported_name' => undef, 'coral_name' => undef};

        # create new Org in Coral
        my $org_input_clean = $org_input_name;
        if ($titlecase) {
            $org_input_clean = lc($org_input_clean);
            # capitalize first letter of each word
            #$org_input_clean =~ s/\b(\w)/\u$1/g; #doesn't work with binary chars
            $org_input_clean =~ s/^(\w)/\u$1/; #capitalize first letter of string
            $org_input_clean =~ s/([-\s])(\w)/$1\u$2/g; #capitalize first letter after hyphen or space
        }
        my $rows_affected = $qh_new_org->execute($org_input_clean) if $UPDATE_DB;

        if ($rows_affected == 1 or !$UPDATE_DB) {
            $org->{'id'} = $org_dbh->last_insert_id(0, 0, 0, 0); #0s prevent error about expected params, but mysql appears to ignore them (re: DBI docs in cpan)
            $org->{'imported_name'} = $org_input_clean;
            $count_orgs_created++;
        } else {
            warn "QUERY ERROR: Cannot create Org: $org_input_name\n";
        }
        $orgs{$org_standard_name} = $org;
    }

    return $org->{'id'};
}


# FUNCTION: Create or SKIP a resource if found (matching on ISSN)
# - don't overwrite any data
sub create_res {
    my ($org_ids_ref, $params) = (shift, shift);
    my $title = $params->{'title'};
    my $issn = $params->{'issn'};
    my $url = $params->{'url'};
    my $price = $params->{'price'};
    my $acq_type_id = undef;
    if (defined $price) {
        $acq_type_id = ($price > 0) ? $acq_type_ids{'Paid'} : $acq_type_ids{'Free'}; #if 'Amount' > 0
        # convert dollars to cents ('$40' -> '4000', '50.5' -> '5050')
        #$price =~ s/^\$?(\d+)$/$1.00/; #if no decimal, add it
        #$price =~ s/^\$?(\d+)\.(\d)$/$1.${2}0/; #if incomplete decimal, add '0'
        $price =~ s/[\$\.,]//g; #remove punctuation
    }
    my $fund = $params->{'fund'};
    my $order_type = $order_types{$params->{'order_type'}};
    my $purch_site_id = $purchase_sites{$params->{'purchasing_site'}};
    my $res_id = 0;

    # if ISSN column is blank, use alternate if supplied
    if (!$issn and $params->{'alt_issn'}) {
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
            # capitalize first letter of each word
            #$title =~ s/\b(\w)/\u$1/g; #doesn't work with binary chars
            $title =~ s/^(\w)/\u$1/; #capitalize first letter of string
            $title =~ s/([-\s])(\w)/$1\u$2/g; #capitalize first letter after hyphen or space
        }
        print "Creating new Resource: [$standardized_issn] \{$fund: $price, $order_type, $purch_site_id} $title ($url)\n";

        my $rows_affected = $qh_new_res->execute($title, $standardized_issn, $url) if $UPDATE_DB;
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

            # add payment info, if provided
            if ($price and $fund) {
                my $result = $qh_new_res_pymt->execute($res_id, $fund, $price, $order_type) if $UPDATE_DB;
                $count_pymt_added++ if ($result or !$UPDATE_DB);
            }

            # add purchasing site info, if provided
            if ($purch_site_id) {
                $qh_new_res_purch->execute($res_id, $purch_site_id) if $UPDATE_DB;
            }
        }
    }
    return $res_id;
}


# FUNCTION: Clean up and standardize the Org name
sub standardize_org_name {
    my $name = shift;
    $name = uc($name);

    # REMOVE punctuation, common words, abbrevs, extra spaces
    $name =~ s/[&,'\.:]//g;
    $name =~ s/-/ /g;
    $name =~ s/\b(CO|INC|LLC|LTD|GMBH|AND|FOR|OF|AT|A|THE)\b//g; #often omitted
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

# ONE-TIME USE: FOR Project Muse data load
    $name =~ s/\bPENN\b/PENNSYLVANIA/i;
    $name =~ s/\bNORTH AMERICAN SOCIETY SPORT HIST\b/NORTH AMERICAN SOCIETY SPORT HISTORY/i;
    $name =~ s/\bMURPHY INSTITUTE\/CITY UNIVERSITY NEW YORK\b/JOSEPH A MURPHY INSTITUTE CITY UNIVERSITY NEW YORK/i;
    $name =~ s/\bMOSAIC JOURNAL INTERDISCIPLINARY STUDY LITERATURE\b/MOSAIC/i;
    $name =~ s/\bMID AMERICAN STUDIES ASSOCIATION\b/MID AMERICA AMERICAN STUDIES ASSOCIATION/i;
    $name =~ s/\bMELUS SOCIETY STUDY MULTI ETHNIC LITERATURE UNITED STATES\b/MELUS/i;
    $name =~ s/\bCENTER IRISH STUDIES UNIVERSITY ST THOMAS\b/CENTER IRISH STUDIES/i;
    
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

