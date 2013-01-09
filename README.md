coral-import-scripts
====================
Basic generic and custom import/dataloader scripts

Quick Start Guide
-------------
Run these commands in a terminal to download and setup the files:
```bash
wget https://raw.github.com/hektech/coral-import-scripts/master/coral_generic_import.pl
chmod 744 coral_generic_import.pl
wget https://raw.github.com/hektech/coral-import-scripts/master/coral_db.conf
vi coral_db.conf
```
Now add your basic CORAL database user credentials to the configuration file.  Then run this command to test that everything is working:
```bash
./coral_generic_import.pl
```
If you get the "usage" message, then perl is working.  Now you can use parameters to tell the script which fields in your data file contain each important piece of data.  Consult the "usage" message for details.  Here is an example:
```bash
./coral_generic_import.pl -f=path/to/data_file.csv -title=0 -issn=3 -alt_issn=4 \
-url=2 -publisher=1 -platform='Name of Platform' -vendor='Name of Vendor' -conf=path/to/coral_db.conf
```

General Instructions
--------------------
I encourage everyone to look through the code before you run a script.  You are free to change anything you like, and please share with the CORAL Discussion list (CORAL-ERM@listserv.nd.edu) any bug fixes or improvements that you make.  And as always with things like this, always test on a copy of your DB and/or backup your DB before running these scripts.  This code is provided "as is" without warranty of any kind.

Getting the Scripts
-------------------
In the GitHub repository (https://github.com/hektech/coral-import-scripts), you will find two perl scripts and a configuration file (coral_db.conf).  You can download them at these URLs:

* https://raw.github.com/hektech/coral-import-scripts/master/coral_generic_import.pl 
* https://raw.github.com/hektech/coral-import-scripts/master/coral_ebsco_import.pl 
* https://raw.github.com/hektech/coral-import-scripts/master/coral_db.conf

The EBSCO script and the generic script keep gaining more in common.  The main differences now are: 

* the EBSCO import requires more fields (as the script will tell you if you neglect any)
* it adds a custom EBSCO link to each resource (using EBSCO title number) and organization (using EBSCO publisher number)
* it handles organization and resource titles in a special way (since many EBSCO titles are abbreviated).  I did my best to match all organization titles from EBSCO with those that come with the CORAL Organizations module (which explains all the custom regex's in the function standardize_org_name).  

Also, there are three things I have not yet included in the scripts that I found helpful:

* added AliasType: 4 = "EBSCO Name" (EBSCO script adds aliases of this type, whether the type exists or not)
* added ResourceFormat: 4 = "Other" (to handle anything that isn't Print/Online/Print+Online)
* added User: system = System Auto (for anywhere that displays creator's first and last name)

It is easiest to put all three files in the same directory on your machine, but it doesn't matter where, as long as that directory is able to run perl scripts.  You may need to make the scripts executable, like this:

    chmod 744 coral_generic_import.pl

Test that you can execute the script by trying to run it (with no parameters).  This command assumes you are in the same directory as the script:

    ./coral_generic_import.pl

or on a Mac, you could try: 

    perl coral_generic_import.pl

You should get a "usage" message, telling you how to run the script.  If you get some other message instead, then something is wrong with your setup.  If you haven't run perl scripts on your machine before, you may need to install the specific perl packages that these scripts use (DBI, Text::CSV, and Getopt::Long).  If you need help with that, try [this article](http://triopter.com/archive/how-to-install-perl-modules-on-mac-os-x-in-4-easy-steps/) (including the comments section, if needed).

It is also helpful to place your CSV data file in the same directory as the scripts.  The only format requirement for the data file is that the first row of the data file will be the header row, and after that everything is data for importing.

Next, you need to enter your CORAL database credentials in coral_db.conf.  Use the username/password that normally accesses the CORAL database, the basic one with only Select/Insert/Update/Delete privileges.

Lastly, there are two built-in constants that you need to know about: **$UPDATE_DB** (which controls whether the script really changes the database or only pretends) and **$DEBUG** (which controls the level of output).  Both are binary flags set in the code, and both are set to zero by default (meaning no changes to the DB, and no debug output).  You can change them in either script, but make sure you know when you are really changing the DB, and always test on a copy of your DB and/or backup your DB before running these scripts.

Here are sample commands for running each of the import scripts.

Run a generic import on Project Muse data:

    ./coral_generic_import.pl -f=../../coral/project_muse_utf8.csv -title=0 -issn=3 -alt_issn=4 \
    -url=2 -publisher=1 -platform='Project Muse' -vendor='Project Muse' -conf=../../coral/coral_db.conf

Run the EBSCO import:

    ./coral_ebsco_import.pl -f=../../coral/summary_of_pub.csv -title=0 -title_num=1 -issn=2 \
    -format=3 -price=12 -publisher=35 -pub_num=37 -url=42
