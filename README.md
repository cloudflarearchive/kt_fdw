# Blackhole Foreign Data Wrapper for PostgreSQL

This skeleton FDW simply throws away any data it receives. Updates and deletes
are no-ops, since there is no data to update or delete. Selecting data from it 
returns an empty resultset, of course.

This code as partly written as a bit of fun for pgCon 2013, and partly so there
would be a skeleton FDW that could be used as the basis for creating another
FDW. Simply take this code, globally replace the word "blackhole" (preserving
case) with your FDW's prefix and you will have a new functional FDW you can
start adding data handling features to.

The code includes the new new data modifying stuff as well as the pre-9.3 data
scanning functionality.

The comments in the functions come from the draft 9.3 docs as at the time of
writing (May 26 2013). That saves you from having to go back and forth
between the docs and the code.
