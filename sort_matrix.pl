#!/usr/bin/perl
use v5.10; 
use strict;
use warnings;

# This script will arrange the rows and columns of a matrix in the sorted order :-)

my @row;

while(my $line = <DATA>)
{
   push @row, [split(/\s+/,$line)];
}      

#Sort element with same length, exchange position
foreach my $r1 (0 .. $#row)
{
    #Take 1 row as base, check other rows
    my @remain = grep !/$r1/,(0 .. $#row);
    foreach my $r2 (0..$#remain)
      {
       # Foreach element in row, find element with same length
       # in different row (mean same column) -> check value and exchange if needed
       foreach my $c1 (0..$#{$row[$r1]})
          {
                 foreach my $c2 (0..$#{$row[$r2]})
                 {
                    # compare_wrap($row[$r1][$c1], $row[$r2][$c2]);
                   
                    print "$row[$r1][$c1]  ";
                    print "$row[$r2][$c2]\n";

                    if ($row[$r1][$c1] < $row[$r2][$c2])
                       {
                        my $temp=$row[$r1][$c1];
                        $row[$r1][$c1]=$row[$r2][$c2];
                        $row[$r2][$c2]=$temp;
                       }
                 }
            }
      }
}

map {print "@$_\n"} @row;
#SUPPORT FUNCTION

__DATA__
1014	1	10
1015	51	100
1016	11	50
1017	101	999
