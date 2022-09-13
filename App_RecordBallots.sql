--After building the database, these steps would generate 20,000 ballots for the default election.
call sim_ballots_bulk(1,10000,1000,'precinct A');
call sim_ballots_bulk(1,10000,1000,'precinct B');
update election set ballotingclosed=true where id=1;
call TabulateBallotsForOffice(1,1);
call TabulateBallotsForOffice(2,1);
call TabulateBallotsForOffice(3,1);
call TabulateBallotsForOffice(4,1);
call TabulateBallotsForOffice(5,1);
call TabulateBallotsForOffice(6,1);
select * from vResultsPivot where election_id=1;


