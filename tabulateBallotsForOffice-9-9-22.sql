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
	RAISE INFO 'Deleted % rows, to start new tabulation results for office id: % and ElectionID: %.', num_upd, offioieID, electionID;

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
