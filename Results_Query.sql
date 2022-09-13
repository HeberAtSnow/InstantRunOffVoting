
with 
pref1 as (
select ballot_id,co.office_id,case 
when co.eliminated_tf is true then null else candidate_office_id end c_o_id  from
ballot_pref bp inner join candidate_office co 
on (bp.office_id=co.office_id and bp.candidate_office_id=co.id)
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
	pref1 full join pref2 on (pref1.ballot_id=pref2.ballot_id and pref1.office_id=pref2.office_id)
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
select eff_ballot.office_id , effective_c_o_id, 
count(*) as numvotes,
count(*)/(cast(co_votes.office_total_votes as decimal))*100 as PctEffectiveBallots,
 co_votes.office_total_votes
from eff_ballot inner join co_votes on (co_votes.office_id=eff_ballot.office_id)
group by eff_ballot.office_id,effective_c_o_id,office_total_votes
order by 1,3 desc






------- Query for testing/debugging IRO logic
------- Query for testing/debugging IRO logic
------- Query for testing/debugging IRO logic
------- Query for testing/debugging IRO logic



with round0 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '0'
),
round1 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '1'
),
round2 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '2'
),
round3 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '3'
),
round4 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '4'
),
round5 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '5'
),
round6 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '6'
),
round7 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '7'
),
round8 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '8'
),
round9 as 
(	select t.c_o_id c_o_id, voting_round voting_round , votesreceived , pctvotesforoffice 
from iro.tabulationresult t inner join candidate_office c_o on (c_o.id=t.c_o_id)
where office_id=5 and voting_round like '9'
)
select 
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
c_o.office_id=5
order by 1;
