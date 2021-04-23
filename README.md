## 2 PHASE MERGE SORT

## Algorithm to sort a large number of records.

## Programming Language Used - ​Python

# Implementation Specs:

1. The metadata file will contain information about the size of the different columns (in bytes).
2. The data type for all columns is a string.
3. The number of columns range from 1 to 20.
4. Capable ​​​of​​​ sorting ​​​in ​​​both ​​​ascending ​​​and​​​ descending​​​ order.
5. Runs for different values of main memory usage allowed and the different size of files (MBs-GBs).

# Command to run program
$> ./sort input.txt output.txt 50 asc C1 C


**Output:**
###start execution
##running Phase-
Number of sub-files (splits): 10
sorting #1 sublist
Writing to disk #
.....
Sorting #10 sublist
Writing to disk #
##running phase-
Sorting...
Writing to disk
###completed execution

# Part-2

Reimplement two-phase merge sort by the ​​ **parallelising first phase of two-phase merge** ​ ​ **sort** ​​​using threads.

# Input format:
## Metadata.txt
<ColumnName​​​​1>,<Size​​​​of​​​​the​​​​column>
<ColumnName​​​​2>,<Size​​​​of​​​​the​​​​column>
........
<ColumnName​​​​n>,<Size​​​​of​​​​the​​​​column>


## Input.txt
Containing the records with the column values. All the values will be string only and might contain space or “,”.

## Data
gensort​ code is used to generate data. 

The following command generates the first 100 tuples of three columns.
$> ./gensort a 100 input.txt

# Command-line inputs:

1.Input file name (containing the raw records)
2.Output filename (containing sorted records)
3.Main memory size (in MB)
4.Number of threads (for part-2)
5.Order code (asc / desc) asc : ascending, desc : descending.
6.ColumnName K
7.ColumnName K
8. .....

# Example

```
● ./sort input.txt output.txt 50 asc C1 C2 (for part1)
● ./sort input.txt output.txt 100 5 desc C3 C1 (for part2)
```

In the first one, the records in input.txt to be sorted in ascending order with 50MB space based on C1 and if any row has the same value of C1, sort based on C2.
