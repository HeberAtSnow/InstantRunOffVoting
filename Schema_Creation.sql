Create schema if not exists iro;

set search_path='iro';

drop view if exists vResultsPivot;
drop view if exists vEffectiveBallot;
drop table if exists tabulationresult;
drop table if exists ballot_pref;
drop table if exists ballot;
drop table if exists candidate_oie;
drop table if exists party;
drop table if exists candidate;
drop table if exists office_in_election;
drop table if exists election;
drop table if exists office;
Drop table if exists state;
Drop table if exists city;
Drop table if exists county;
drop sequence if exists voter_id_seq;

create sequence voter_id_seq increment by 1 start with 111;

create table county (id serial primary key, county_name varchar(80), county_description varchar(80),  contact_title varchar(80), contact_name varchar(80), contact_email varchar(200), contact_phone varchar(80));

create table city (id serial primary key, city_name varchar(80), city_description varchar(80),  contact_title varchar(80), contact_name varchar(80), contact_email varchar(200), contact_phone varchar(80));

create table state(id serial primary key, state_name varchar(80), state_description varchar(80),  contact_title varchar(80), contact_name varchar(80), contact_email varchar(200), contact_phone varchar(80));

create table office (id serial primary key, office_name varchar(80), county_id integer, city_id integer, state_id integer, positions_num integer, election_id integer, constraint city_county_state_office_validation 
CHECK ( (county_id IS NOT null and city_id IS NULL and state_id IS NULL) or (county_id IS null and city_id IS NOT NULL and state_id IS NULL) or (county_id IS  null and city_id IS NULL and state_id IS NOT NULL) )
);
Alter table office add constraint office_city_fk FOREIGN KEY (city_id) REFERENCES city(id);
Alter table office add constraint office_state_fk FOREIGN KEY (state_id) REFERENCES state(id);
Alter table office add constraint office_county_fk FOREIGN KEY (county_id) REFERENCES county(id);

create table election (id serial primary key, earlyvotingbegin date, earlyvotingend date, poll_date date, ballotingclosed bool not null);

create table office_in_election (id serial primary key, office_id integer, election_id integer not null, quantity integer default 1);
alter table office_in_election add constraint office_election_election_fk FOREIGN KEY(election_id) REFERENCES election(id);
alter table office_in_election add constraint office_election_office_fk FOREIGN KEY(office_id) REFERENCES office(id);
create unique index office_in_election_nodup_UIX on office_in_election(election_id,office_id); --prevent duplicates

create table candidate (id serial primary key, candidate_name varchar(200) not null, candidate_email varchar(200), candidate_phone varchar(80), candidate_photo bytea);

create table party (id serial primary key, party_name varchar(80) not null, party_ballot_label varchar(20) not null);
create unique index party_label_UIX on party(party_ballot_label); --prevent duplicates

create table candidate_oie (id serial primary key, candidate_id integer, oie_id integer, eliminated_tf bool not null, filed_date date not null, party_id integer, candidate_type varchar(20) default 'printed' );
alter table  candidate_oie add constraint candidate_oie_candidate_FK FOREIGN KEY (candidate_id) REFERENCES candidate(id);
alter table  candidate_oie add constraint candidate_oie_party_FK FOREIGN KEY (party_id) REFERENCES party(id);
alter table  candidate_oie add constraint candidate_oie_office_in_election_FK FOREIGN KEY (oie_id) REFERENCES office_in_election(id);
alter table  candidate_oie add constraint candidate_type_restriction CHECK(candidate_type in ('printed','write-in'));
create unique index candidate_oie_nodup_UIX on candidate_oie(oie_id, candidate_id); --prevent duplicates

create table ballot (id serial primary key, voter_id int, precinctInfo text, cast_timestamp timestamp);

create table ballot_pref(id serial primary key, ballot_id int, preference_num int, candidate_oie_id int);
alter table ballot_pref add constraint ballot_pref_ballot_FK FOREIGN KEY (ballot_id) REFERENCES ballot(id);
alter table ballot_pref add constraint ballot_pref_candidate_oie_FK FOREIGN KEY (candidate_oie_id) REFERENCES candidate_oie(id);
Create unique index ballot_candidate_OIE_UIX on ballot_pref(ballot_id,candidate_oie_id); --prevent duplicate votes 

create table tabulationresult (id serial primary key, candidate_oie_id integer, voting_round varchar(20), votesreceived integer, pctvotesforoffice decimal(8,5));
alter table tabulationresult add constraint tabulation_candidateoffice_FK FOREIGN KEY (candidate_oie_id) REFERENCES candidate_oie(id);




--Voter Simulation (ballot generation) Logic

create or replace procedure sim_ballots_bulk(electionID int, num_votes int, commit_every int, precinct_input varchar(80)) language plpgsql as $$
declare 
  ballots_done int;
  my_cur_ballot int;
  eachOfficeInElection record;
  my_rownum int; my_election_rows int;
  my_row candidate_oie%ROWTYPE;
  c1 cursor (oieID int) for select coie.id,coie.candidate_id,coie.oie_id from candidate_oie coie inner join office_in_election oie on (coie.oie_id=oie.id) where oie.election_id=electionID and coie.oie_id=oieID order by random();
begin
	if num_votes <=0 or num_votes is null then raise EXCEPTION 'Can not call this procedure with num_votes <= 0.  Set num_votes to how many ballots(voters) you want to simulate.'; end if;
	if commit_every is null or commit_every <=0 then raise EXCEPTION 'Can not call this procedure.  Commit point (commit_every) must be >=0.'; end if;
	--validate electionID
	----ensure election exists and is open for voting
	----ensure candidates have been configured for the election
	select count(*) into my_election_rows from election where id=electionID and ballotingclosed=false;
	if ( my_election_rows != 1 or electionID is null ) then raise EXCEPTION 'Can not call this procedure with bad electionID.  ElectionID must not be null and ballotingclosed must be FALSE.'; end if;
	select count(*) into my_election_rows from election e inner join office_in_election oie on (oie.election_id=e.id) inner join candidate_oie coie on (coie.oie_id=oie.id) where e.id=electionID;
	if (my_election_rows <1) then raise EXCEPTION 'Can not call this procedure.  Election is missing candidates configured to offices in the election.'; end if;

	ballots_done:=0;
	while ballots_done < num_votes
	loop
		insert into ballot(voter_id,precinctinfo,cast_timestamp) values(   nextval('voter_id_seq'), precinct_input,now())
		   returning id into my_cur_ballot;
		FOR eachOfficeInElection in select distinct oie_id from candidate_oie coie inner join office_in_election oie on (coie.oie_id=oie.id) where election_id=electionID
		loop
			open c1(eachOfficeInElection.oie_id); 
			my_rownum:=1;--firstPref=1
			loop --each CANDIDATE in the given office (each row in candidate_oie)
				fetch c1 into my_row;
				exit when not found;
				insert into ballot_pref (ballot_id,preference_num,candidate_oie_id)
				values (my_cur_ballot, my_rownum, my_row.id);
				my_rownum := my_rownum +1;
			end loop;
			close c1;
		END LOOP;
	   ballots_done := ballots_done +1;
	   if(ballots_done%commit_every = 0) then commit; end if;
	end loop;
	commit; --catch any leftover records
end
$$
;





create or replace view vEffectiveBallot (election_id, coie_id, oie_id , candidate_id, numvotes, PctEffectiveBallots, office_total_votes) as (
with 
pref1 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num =1 
),
pref2 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=2 
),
pref3 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=3 
),
pref4 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=4 
),
pref5 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=5 
),
pref6 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=6 
),
pref7 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=7 
),
pref8 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=8 
),
pref9 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=9 
),
pref10 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=10 
),
pref11 as (
select ballot_id, coie.oie_id, case when eliminated_tf is true then null else candidate_oie_id end coie_id  
from ballot_pref bp inner join candidate_oie coie on (bp.candidate_oie_id=coie.id)
where preference_num=11 
),
eff_ballot as (
select pref1.ballot_id,
		pref1.oie_id,
       coalesce(pref1.coie_id,pref2.coie_id,pref3.coie_id, pref4.coie_id,pref5.coie_id,
       		pref6.coie_id,pref7.coie_id,pref8.coie_id,pref9.coie_id,pref10.coie_id,pref11.coie_id
       ) effective_coie_id
from
	pref1 
	full join pref2 on (pref1.ballot_id=pref2.ballot_id and pref1.oie_id=pref2.oie_id)
	full join pref3 on (pref1.ballot_id=pref3.ballot_id and pref1.oie_id=pref3.oie_id)
	full join pref4 on (pref1.ballot_id=pref4.ballot_id and pref1.oie_id=pref4.oie_id)
	full join pref5 on (pref1.ballot_id=pref5.ballot_id and pref1.oie_id=pref5.oie_id)
	full join pref6 on (pref1.ballot_id=pref6.ballot_id and pref1.oie_id=pref6.oie_id)
	full join pref7 on (pref1.ballot_id=pref7.ballot_id and pref1.oie_id=pref7.oie_id)
	full join pref8 on (pref1.ballot_id=pref8.ballot_id and pref1.oie_id=pref8.oie_id)
	full join pref9 on (pref1.ballot_id=pref9.ballot_id and pref1.oie_id=pref9.oie_id)
	full join pref10 on (pref1.ballot_id=pref10.ballot_id and pref1.oie_id=pref10.oie_id)
	full join pref11 on (pref1.ballot_id=pref11.ballot_id and pref1.oie_id=pref11.oie_id)
),
office_votes as (
	select oie_id,count(*) as office_total_votes from eff_ballot group by oie_id
)
select	
	oie.election_id election_id,
	coie.id coie_id,
	coie.oie_id,
	coie.candidate_id,
	count(*) as numvotes,--numvotes
	count(*)/(cast(office_votes.office_total_votes as decimal))*100 as PctEffectiveBallots,
	office_votes.office_total_votes
from
	eff_ballot inner join 
	candidate_oie coie on (eff_ballot.effective_coie_id=coie.id) inner join 
	office_in_election oie on (oie.id=coie.oie_id) inner join
	office_votes on (office_votes.oie_id=oie.id)
group by 1,2,3,4,7
order by 1,3,4 desc
)
;



create or replace procedure TabulateBallotsForOffice(oieID int,electionID int) language plpgsql as $body$
declare 
	roundnumber int := 0; num_upd int; num_oies int;
	ballots_closed bool;
	largestpercent decimal;
	lowestvotes decimal;
	lowestpercentcountcandidates int;
	lowestpercent_c_o_id int;
	countofcandidates int;
	c_o_idwinner int;
	votesrecievedwinner int;
	pctvotesforofficewinner decimal;
	numEffCandidatesInOffice int := 0;
	numCandidatesWithLowestVotes int := 0;
	
begin 
	select e.ballotingclosed into ballots_closed from election e 
		where e.id = electionID;
	if ballots_closed is not true then
		RAISE EXCEPTION 'Can not calculate ballots before election closes. Update Election.AcceptingBallots before proceeding.';
		RETURN;
	end if;

	select count(*) into num_oies from office_in_election oie where election_id=electionID and id=oieID;
	if num_oies <1 then raise EXCEPTION 'Procedure failure.  Provided oieID is not valid with this election_id.'; RETURN; end if;

	delete from tabulationresult t where t.candidate_oie_id in (
		select coie.id 
		from 
		office_in_election oie inner join candidate_oie coie on (oie.id=coie.oie_id)
		where oie.election_id=electionID and oie.id=oieID );
	get diagnostics num_upd = row_count ;
	RAISE INFO 'Deleted % rows, to start new tabulation results for office id: % and ElectionID: %.', num_upd, oieID, electionID;
    	update candidate_oie set eliminated_tf = false where oie_id = oieid;
	RAISE INFO 'Round number is %',roundnumber;
	loop
		--if any candidates have NULL votes and they are still 'active' then mark them as inactive
		update candidate_oie coie set eliminated_tf =true 
		where eliminated_tf is false 
			and id in 
			(
			select coie.id
			from
				candidate_oie coie
			full outer join veffectiveballot v on (coie.id=v.coie_id)
			where eliminated_tf = false 
			and v.numvotes is null 
			);
		get diagnostics num_upd = row_count ;
		raise INFO '% candidates had NO VOTES.  Now they are marked as eliminated', num_upd;
	
		select max(vEB.pcteffectiveballots) into largestpercent
		from vEffectiveBallot vEB inner join candidate_oie coie on (VEB.coie_id=coie.id)
		where vEB.oie_id=oieID ; --note, safety, don't need electionID because of oieID/electionID validation earlier 
		RAISE Info 'Largest percent: % in round: %', largestpercent, roundnumber;
	
		insert into tabulationresult
			(candidate_oie_id, voting_round, votesreceived, pctvotesforoffice) 
		select vEB.coie_id, roundnumber, numvotes, PctEffectiveBallots
			from vEffectiveBallot vEB inner join candidate_oie coie on (coie.id=vEB.coie_id)
			inner join office_in_election oie on (oie.id=coie.oie_id)
			where vEB.oie_id=oieID and oie.election_id=electionID;

		--Do we have a winner >50.0%
		if largestpercent >= 50.0 then
			RAISE INFO 'Done!  A candidate has >50.0 percent of votes.  Ending.  Pctvotes: %.', largestpercent;
			exit;
		end if;
	
		--Remove lowest candiate(s) as long as it would leave at least 2 candidates left in the race
		select min(numvotes) into lowestvotes  from vEffectiveBallot vEB inner join candidate_oie coie on (coie.id=vEB.coie_id)
			inner join office_in_election oie on (oie.id=coie.oie_id)
			where vEB.oie_id=oieID and oie.election_id=electionID;
		select count(*) into numEffCandidatesInOffice from vEffectiveBallot vEB inner join candidate_oie coie on (coie.id=vEB.coie_id)
			inner join office_in_election oie on (oie.id=coie.oie_id)
			where vEB.oie_id=oieID and oie.election_id=electionID;
		select count(*) into numCandidatesWithLowestVotes from vEffectiveBallot vEB inner join candidate_oie coie on (coie.id=vEB.coie_id)
			inner join office_in_election oie on (oie.id=coie.oie_id)
			where vEB.oie_id=oieID and oie.election_id=electionID and numvotes=lowestvotes;
		
		IF numEffCandidatesInOffice-numCandidatesWithLowestVotes >= 2 then --numEffCandidatesThatWouldRemain
			update candidate_oie coie set eliminated_tf=true 
			FROM vEffectiveBallot vEB 
			WHERE coie.id=vEB.coie_id
			and   vEB.oie_id=oieID
			and   vEB.election_id=electionID
			and	  vEB.numvotes=lowestvotes;
			get diagnostics num_upd = row_count ;
			raise INFO 'Marked % candidates as eliminated', num_upd;
		else
			RAISE INFO 'Done!  Can not eliminate any more candidates without leaving 2 (or more) candidates left.  office_id: %, election_id: %, roundNumber: %.',officeID,electionID,roundnumber;
			exit;
		end if;

		roundnumber := roundnumber +1;
		RAISE INFO 'Raised round number to: %',roundnumber;
	end loop;
end;
$body$
;



create or replace view vResultsPivot as (
with round0 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '0'
),
round1 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '1'
),
round2 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '2'
),
round3 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '3'
),
round4 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '4'
),
round5 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '5'
),
round6 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '6'
),
round7 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '7'
),
round8 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '8'
),
round9 as 
(	select t.candidate_oie_id coie_id, voting_round, votesreceived , pctvotesforoffice from tabulationresult t 
where voting_round like '9'
),
localitynames as (
select office.id office_id,city_name as locality from city inner join office on (office.city_id=city.id)
union
select office.id office_id,county_name as locality from county inner join office on (office.county_id=county.id)
union 
select office.id office_id,state_name as locality from state inner join office on (office.state_id=state.id)
)
select 
oie.election_id ,
o.office_name ,o.id office_id, localitynames.locality,
c.candidate_name,
coie.candidate_id, 
round0.votesreceived InitialRoundVotes, trunc(round0.pctvotesforoffice,3) InitialRoundPct,
round1.votesreceived Round1Votes, trunc(round1.pctvotesforoffice,3) Round1Pct,
round2.votesreceived Round2Votes, trunc(round2.pctvotesforoffice,3) Round2Pct,
round3.votesreceived Round3Votes, trunc(round3.pctvotesforoffice,3) Round3Pct,
round4.votesreceived Round4Votes, trunc(round4.pctvotesforoffice,3) Round4Pct,
round5.votesreceived Round5Votes, trunc(round5.pctvotesforoffice,3) Round5Pct,
round6.votesreceived Round6Votes, trunc(round6.pctvotesforoffice,3) Round6Pct,
round7.votesreceived Round7Votes, trunc(round7.pctvotesforoffice,3) Round7Pct,
round8.votesreceived Round8Votes, trunc(round8.pctvotesforoffice,3) Round8Pct,
round9.votesreceived Round9Votes, trunc(round9.pctvotesforoffice,3) Round9Pct
from 
	office o inner join
	office_in_election oie on (o.id=oie.office_id) inner join
	candidate_oie coie on (coie.oie_id=oie.id) inner join
	candidate c on (c.id=coie.candidate_id)
	left join round0 on (coie.id=round0.coie_id)
	left join round1 on (coie.id=round1.coie_id)
	left join round2 on (coie.id=round2.coie_id)
	left join round3 on (coie.id=round3.coie_id)
	left join round4 on (coie.id=round4.coie_id)
	left join round5 on (coie.id=round5.coie_id)
	left join round6 on (coie.id=round6.coie_id)
	left join round7 on (coie.id=round7.coie_id)
	left join round8 on (coie.id=round8.coie_id)
	left join round9 on (coie.id=round9.coie_id)
	left join localitynames on (localitynames.office_id=o.id)
order by 1,3,4 
);

