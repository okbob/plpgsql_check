custom_scan_function
====================

This creates a custom function, as an example, that changes the output to better fit my needs inside of my IDE (DataGrip)

# Usage
Through a macro that parses the current function, I call this with ONE routine: 
* `select * from plpgsql_check_custom('plpgsql_check_custom'::regproc);`


Without that parameter, it runs for all routines!  There are more parameters to easily control the intensity, or run for a set of schemas or filter messages!
This is a starting point... Make it your own.

# Why the timestamp
FWIW, I return the timestamp ts with the message because I have 10+ windows open, each with it's own code+output.  And after fixing a bunch of stuff in another window, that timestamp ALSO tells me how "dated" the scan is.  It also confirms it refreshed (sometimes it runs so fast, and gives you the same output, you are not sure if it actually refreshed!).  The great part is that once you have a RECORD() type setup for output, adding more columns is easy.

# Why the procedure name as a row and not a column
Honestly, we have horribly long names: long_schema_name.Really_Long_package_name.Really_long_function_name()!
While they are clear and make coding easier, it is quickly a waste of screen real-estate.  I would rather have *one* long column in my output.
It's a personal preference.  And that is the beauty of PG and of this tool.

# Motivation
Finally, for output, the custom message that made me do this is given as an example below.  The message from the base level was NOT enough.
It only tells you what the code is trying to do.  It does not make it clear WHICH parameter is likely wrong.  So through a bit of introspection (Thanks to Pavel),
I was able to add the full parameter details (including DEF it that parameter has a DEF value).  As well as the expected TYPE...

# Output
`
#### 
#### Error in: schema.pkg_name$update_user() at Line: 16       PARAMETER TYPING ISSUE?
#### 
#### Param Name           Flow/DEF  (your code)    Definition     
#### ==========           ========  ===========    ==========     
#### eid                  IN        bigint         bigint         
#### pid                  IN        bigint         bigint         
#### typ                  IN        character varyibigint         
#### val                  IN        bigint         text           
#### addrmv               IN        bigint         integer        
#### 
`

# Future Ideas
Now that this actually exists, I have a few more ideas.  I will actually see how to integrate this better with my DataGrip.
I would love to make this work in psql, as a setting: 
`\set PLPGSQL_CHECK_ALTERED_ROUTINES ON`

So, whenever I successfully compile (create or replace) a routine... Then this would run and output the issues it finds!

There are a couple of additional things I would like to do.  Some error messages give a line number, but it does not match up.
I would love to do some code introspection, and extract the LINE in question (or better yet, the field in the case of FETCH INTO ... mismatch)
