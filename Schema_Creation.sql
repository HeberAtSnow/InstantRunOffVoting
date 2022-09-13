Create schema if not exists iro2;

set search_path='iro2';

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

create table office_in_election (id serial primary key, office_id integer, election_id integer, quantity integer default 1);
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





create view vEffectiveBallot (office_id , c_o_id, numvotes, PctEffectiveBallots, office_total_votes) as (
with 
pref1 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id  from
ballot_pref bp inner join candidate_oie coie
on (bp.candidate_oie_id=coie.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num =1 
),
pref2 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=2 
),
pref3 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=3 
),
pref4 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=4 
),
pref5 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=5 
),
pref6 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=6 
),
pref7 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=7 
),
pref8 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=8 
),
pref9 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=9 
),
pref10 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=10 
),
pref11 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
--inner join vars on (vars.curr_office=co.office_id)
where preference_num=11 
),
eff_ballot as (
select pref1.ballot_id,pref1.office_id,
       coalesce(pref1.c_o_id,pref2.c_o_id,pref3.c_o_id, pref4.c_o_id,pref5.c_o_id,
       		pref6.c_o_id,pref7.c_o_id,pref8.c_o_id,pref9.c_o_id,pref10.c_o_id,pref11.c_o_id
       ) effective_c_o_id
from
	pref1 
	full join pref2 on (pref1.ballot_id=pref2.ballot_id and pref1.office_id=pref2.office_id)
	full join pref3 on (pref1.ballot_id=pref3.ballot_id and pref1.office_id=pref3.office_id)
	full join pref4 on (pref1.ballot_id=pref4.ballot_id and pref1.office_id=pref4.office_id)
	full join pref5 on (pref1.ballot_id=pref5.ballot_id and pref1.office_id=pref5.office_id)
	full join pref6 on (pref1.ballot_id=pref6.ballot_id and pref1.office_id=pref6.office_id)
	full join pref7 on (pref1.ballot_id=pref7.ballot_id and pref1.office_id=pref7.office_id)
	full join pref8 on (pref1.ballot_id=pref8.ballot_id and pref1.office_id=pref8.office_id)
	full join pref9 on (pref1.ballot_id=pref9.ballot_id and pref1.office_id=pref9.office_id)
	full join pref10 on (pref1.ballot_id=pref10.ballot_id and pref1.office_id=pref10.office_id)
	full join pref11 on (pref1.ballot_id=pref11.ballot_id and pref1.office_id=pref11.office_id)
),
co_votes as (
select office_id,count(*) as office_total_votes from eff_ballot
group by office_id
)
select 
	eff_ballot.office_id , 
	effective_c_o_id, 
	count(*) as numvotes,
	count(*)/(cast(co_votes.office_total_votes as decimal))*100 as PctEffectiveBallots,
	co_votes.office_total_votes
from 
	eff_ballot 
	inner join co_votes on (co_votes.office_id=eff_ballot.office_id)
group by eff_ballot.office_id,effective_c_o_id,office_total_votes
order by 1,3 desc
);







create or replace view vResultsPivot as (
with round0 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '0'
),
round1 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '1'
),
round2 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '2'
),
round3 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '3'
),
round4 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '4'
),
round5 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '5'
),
round6 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '6'
),
round7 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '7'
),
round8 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '8'
),
round9 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where voting_round like '9'
)
select 
c_o.election_id ,
o.office_name ,o.id,
c.candidate_name,
c_o.candidate_id, 
round0.votesreceived InitialRoundVotes, to_char(round0.pctvotesforoffice,'fm00D000%') InitialRoundPct,
round1.votesreceived Round1Votes, to_char(round1.pctvotesforoffice,'fm00D000%') Round1Pct,
round2.votesreceived Round2Votes, to_char(round2.pctvotesforoffice,'fm00D000%') Round2Pct,
round3.votesreceived Round3Votes, to_char(round3.pctvotesforoffice,'fm00D000%') Round3Pct,
round4.votesreceived Round4Votes, to_char(round4.pctvotesforoffice,'fm00D000%') Round4Pct,
round5.votesreceived Round5Votes, to_char(round5.pctvotesforoffice,'fm00D000%') Round5Pct,
round6.votesreceived Round6Votes, to_char(round6.pctvotesforoffice,'fm00D000%') Round6Pct,
round7.votesreceived Round7Votes, to_char(round7.pctvotesforoffice,'fm00D000%') Round7Pct,
round8.votesreceived Round8Votes, to_char(round8.pctvotesforoffice,'fm00D000%') Round8Pct,
round9.votesreceived Round9Votes, to_char(round9.pctvotesforoffice,'fm00D000%') Round9Pct
from 
candidate c 
inner join candidate_office c_o on (c.id=c_o.candidate_id) 
inner join office o on (o.id=c_o.office_id)
left join round0 on (c_o.id=round0.c_o_id)
left join round1 on (c_o.id=round1.c_o_id)
left join round2 on (c_o.id=round2.c_o_id)
left join round3 on (c_o.id=round3.c_o_id)
left join round4 on (c_o.id=round4.c_o_id)
left join round5 on (c_o.id=round5.c_o_id)
left join round6 on (c_o.id=round6.c_o_id)
left join round7 on (c_o.id=round7.c_o_id)
left join round8 on (c_o.id=round8.c_o_id)
left join round9 on (c_o.id=round9.c_o_id)
where 
c_o.election_id = 2
order by c_o.election_id , o.id,c.candidate_name 
);

