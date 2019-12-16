# open-elections v0.1.0.0

First try at parsing [openelections.net](http://openelections.net/) data and re-aggregating
state-wide elections 
by state legislative district for the 2018 elections.

The idea is to automate the process.  Each state has a config
which identifies the file(s) for the primary election--the one
we are re-aggregating and using for matches first--and then file(s),
in preference order, to use for secondary matching.

We're currently using the format that appears in the 
openelections-data-XX repos.  We'll also try the
openelections-results-XX repo style.

Current Issues:

1. State Senate is hard.  Not all offices come up in each election
and (county,precinct) pairs seem to change enough between elections
to stymie efforts to match using older elections

Status:

1. GA:  Pretty good in both house and Senate
2. IA: Good house but Senate needs work (see above)
3. TX: Good house but Senate needs work (see above)
4. PA: WIP
5. WI: WIP
6. NC: WIP, pending work on the "open-elections-results-XX" 
