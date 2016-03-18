#!/usr/bin/perl
# Compare two schemas or tables for names, types and length.
# Author                        Date            Version         Comments
# Raghu.Cherukuru               03/09/2016      v1.0            Initial version
# Raghu.Cherukuru               03/10/2016      v1.1            Made this program strict pragma compatible
# Raghu.Cherukuru               03/10/2016      v1.2            Version prior to DBI changes
# Raghu.Cherukuru		03/11/2016	v1.3		Added DBI code to build source and target data structures for comparision

use v5.10;
use strict;
use DBI;

# Script variables
my $srctabhash_ref;
my $srctab_name;
my $trgtabhash_ref;
my $trgtab_name;
my $srccol_name;
my $trgcol_name;
my %srctabs_lookup=();
my %trgtabs_lookup=();
my %srccol_lookup=();
my %trgcol_lookup=();
my %src_tab_to_cols_hash=();
my %trg_tab_to_cols_hash=();
my %srctab_to_coltype_lookup_hoh=();
my %trgtab_to_coltype_lookup_hoh=();


# DBI variables UPDATABLE Variables.
my $sourcedb="TRAVELER";
my $targetdb="TRAVELER";
my $src_hostname="usemb01d.ada.dcn";
my $trg_hostname="ustravdb03.ada.dcn";
my $src_port="50000";
my $trg_port="50000";
my $source_dsn="DBI:DB2:database=$sourcedb;hostname=$src_hostname;port=$src_port";
my $target_dsn="DBI:DB2:database=$targetdb;hostname=$trg_hostname;port=$trg_port";;
my $src_schema="TRAVELER";
my $trg_schema="TRAVELER";

# DBI Non-updatable variables.
my $src_dbh=DBI->connect($source_dsn, "traveler", "TraVeler") or die "Could not connect to database $sourcedb";
my $trg_dbh=DBI->connect($target_dsn, "traveler", "TraVeler") or die "Could not connect to database $targetdb";
my $src_tab_sth;
my $trg_tab_sth;
my $src_col_sth;
my $trg_col_sth;
my %SOURCE_TAB_DS=();
my %TARGET_TAB_DS=();


sub Build_Source_Ds
   {
    my $src_int_arrayref;
    my @src_table_names;
    $src_tab_sth = $src_dbh->prepare("SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA='$src_schema'");
    $src_tab_sth->execute();

    while (my $tab_row = $src_tab_sth->fetchrow_hashref())
       {
        # Build the SOurce tables list into an array
        push @src_table_names, $tab_row->{TABNAME};
       }

       foreach my $src_tab_name (@src_table_names)
          {
           $src_col_sth = $src_dbh->prepare("SELECT COLNAME,TYPENAME,LENGTH FROM SYSCAT.COLUMNS WHERE TABNAME='$src_tab_name' AND TABSCHEMA='$src_schema'");
           $src_col_sth->execute();

           my %tab_to_cols_hash=();    
    
           # Empty out the table hash before each iteration, so that array will have only one hash corresponding to a table.
           # Remember %tab_to_cols_hash=undef vs %tab_to_cols_hash=() not the same. First one will assign an undef value and key to the hash, nasty...
           %tab_to_cols_hash=(); 
     
           # Need to empty out the hash for each iteration of the table, since datatypes are not unique for each table and column.
           my %cols_to_types_hash=();
           while (my $col_row = $src_col_sth->fetchrow_hashref()) 
              {
               $cols_to_types_hash{$col_row->{COLNAME}}="$col_row->{TYPENAME}"."($col_row->{LENGTH})";
              }
            $tab_to_cols_hash{$src_tab_name}={%cols_to_types_hash};
            push @{$src_int_arrayref}, {%tab_to_cols_hash};
           }

<<'TESTCODE';
       foreach my $int_hashref (@{$src_int_arrayref})
          {
           say "$int_hashref";
           while ( my ($tab_key, $col_hash_value) = each %{$int_hashref} )
              {
               say "Table Name: $tab_key, $col_hash_value";
              }
           say;
          }
TESTCODE

   # Build the final Data structure for source schema
   $SOURCE_TAB_DS{$src_schema}=[@{$src_int_arrayref}];

   } 


sub Build_Target_Ds
   {
    my $trg_int_arrayref;
    my @trg_table_names;
    $trg_tab_sth = $trg_dbh->prepare("SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA='$trg_schema'");
    $trg_tab_sth->execute();

    while (my $tab_row = $trg_tab_sth->fetchrow_hashref())
       {
        # Build the target tables list into an array
        push @trg_table_names, $tab_row->{TABNAME};
       }

       foreach my $trg_tab_name (@trg_table_names)
          {
           $trg_col_sth = $trg_dbh->prepare("SELECT COLNAME,TYPENAME,LENGTH FROM SYSCAT.COLUMNS WHERE TABNAME='$trg_tab_name' AND TABSCHEMA='$trg_schema'");
           $trg_col_sth->execute();

           my %tab_to_cols_hash=();

           # Empty out the table hash before each iteration, so that array will have only one hash corresponding to a table.
           # Remember %tab_to_cols_hash=undef vs %tab_to_cols_hash=() not the same. First one will assign an undef value and key to the hash, nasty... 
           %tab_to_cols_hash=();

           # Need to empty out the hash for each iteration of the table, since datatypes are not unique for each table and column. 
           my %cols_to_types_hash=();
           while (my $col_row = $trg_col_sth->fetchrow_hashref())
              {
               $cols_to_types_hash{$col_row->{COLNAME}}="$col_row->{TYPENAME}"."($col_row->{LENGTH})";
              }
            $tab_to_cols_hash{$trg_tab_name}={%cols_to_types_hash};
            push @{$trg_int_arrayref}, {%tab_to_cols_hash};
           }

<<'TESTCODE';
       foreach my $int_hashref (@{$trg_int_arrayref})
          {
           say "$int_hashref";
           while ( my ($tab_key, $col_hash_value) = each %{$int_hashref} )
              {
               say "Table Name: $tab_key, $col_hash_value";
              }
           say;
          }
TESTCODE

   
   $TARGET_TAB_DS{$trg_schema}=[@{$trg_int_arrayref}];

   }

Build_Source_Ds();
Build_Target_Ds();

$src_dbh->disconnect() or say "Failed to disconnect from $sourcedb";
$trg_dbh->disconnect() or say "Failed to disconnect from $targetdb";

foreach my $srcschema (keys %SOURCE_TAB_DS)
   {
   if (exists $TARGET_TAB_DS{$srcschema})
      {
       say "Schema $srcschema exist in target.\n";
      } else {
       die "Schema $srcschema does not exist in target.\n";
      }
   
   
   # Build look up hashes with tables list for source schema from SOURCE_TAB_DS.
   foreach $srctabhash_ref (@{$SOURCE_TAB_DS{$srcschema}})
      {
       foreach $srctab_name (keys %{$srctabhash_ref})
         {
          # Build the hash for tables in source schema for lookup.
          $srctabs_lookup{$srctab_name}=1;
          
          # Build a hash with keys of source table names and values referencing to the deepest hash with columns information
          $srctab_to_coltype_lookup_hoh{$srctab_name}={%{$srctabhash_ref->{$srctab_name}}};
         }
      }

   
    # Build look up hashes with tables list for target schema from TARGET_TAB_DS.
    foreach $trgtabhash_ref (@{$TARGET_TAB_DS{$srcschema}})
      {
       foreach $trgtab_name (keys %{$trgtabhash_ref})
         {
          # Build the hash for tables in target schema for lookup
          $trgtabs_lookup{$trgtab_name}=1;
          
          # Build a hash with keys of target table names and values referencing to the deepest hash with columns information
          $trgtab_to_coltype_lookup_hoh{$trgtab_name}={%{$trgtabhash_ref->{$trgtab_name}}};
         }
      }
  
 
    foreach $srctab_name(keys %srctabs_lookup) 
       {
       if (! exists $trgtabs_lookup{$srctab_name})
          {
           say "$srctab_name table DOES NOT EXIST in target schema\n";
          }
       else
          {
           say "$srctab_name table exists in target schema";
           # Now lets compare the columns and types for each of those columns
           &col_typediff_checker(\%{$srctab_to_coltype_lookup_hoh{$srctab_name}},\%{$trgtab_to_coltype_lookup_hoh{$srctab_name}});
          }
       }
   }

   # In this function we will compare to see if the column names and types are identical between source and target otherwise complain

sub col_typediff_checker
   {
    my ($source_coltype_hashref, $target_coltype_hashref)=@_;
    my @source_cols=(keys %{$source_coltype_hashref});
    my @target_cols=(keys %{$target_coltype_hashref});
    my $missing_col_ind=0;
     
    if ( @source_cols != @target_cols )
       {
        say "\tNumber of columns for source: " . scalar(@source_cols) . " differ from number of columns on Target: " . scalar(@target_cols);
       }
       else
       {
        say "\tNumber of columns between source: " . scalar(@source_cols) . " and target: " . scalar(@target_cols) . " are same";
       }

    foreach my $srccol_name (keys %{$source_coltype_hashref})
       {
        $srccol_lookup{$srccol_name}=1;
       }

    foreach my $trgcol_name (keys %{$target_coltype_hashref})
       {
        $trgcol_lookup{$trgcol_name}=1;
       }

    # Are the column names same for both source and target and whether all the columns in source are present in target and NOT viceversa

    foreach my $srccol_name(keys %{$source_coltype_hashref})
       {
        if (! exists $trgcol_lookup{$srccol_name})
           {
            ++$missing_col_ind;
            say "\t$srccol_name column is NOT PRESENT in target";
           }
        else
           {
            #say "\t$srccol_name column is present in target";
           }
        }


    # Are the datatypes same for both source and target ? Lets check...

    if (scalar grep { ($source_coltype_hashref->{$_} ne $target_coltype_hashref->{$_}) and (defined $target_coltype_hashref->{$_}) } (keys %{$source_coltype_hashref}))
       {
        say "\t\tAtleast one of the data types between Source and Target columns DID NOT MATCH";

        # Use a while loop if you would like to save memory. Every time each is called it only returns a pair of (key, value) element.

        while (my ($key,$value) = each %{$source_coltype_hashref})
           {
            say "\t\tSource column: $key -> $value, Target column: $key -> $target_coltype_hashref->{$key}" if (($target_coltype_hashref->{$key} ne $value) and (defined $trgcol_lookup{$key}));
           }
       }
    else
       {
        say "\t\tThere are some missing column(s): Otherwise all the remaining column's data types between source and target columns matched" if ($missing_col_ind >= 1);
        say "\tAll the column's data types between source and target columns matched" if ($missing_col_ind == 0);
       } 
     %srccol_lookup=();      # Empty the hash lookup for the next table's columns iteration
     undef %trgcol_lookup;   # Apparently this does the same as above
     say;
    }

