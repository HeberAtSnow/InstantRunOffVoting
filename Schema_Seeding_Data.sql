insert into county (county_name,county_description,contact_title,contact_name,contact_email,contact_phone)
values ('Sanpete County', 'Recorder, Assessor and Treasurer', 'County Clerk','Sandy Neill','sneill@sanpetecountyutah.gov','435-835-2131 Ext. 5');
insert into county (county_name,county_description,contact_title,contact_name,contact_email,contact_phone)
values ('Sevier County','','County Clerk/Auditor','Steven C. Wall','scwall@sevier.utah.gov','(435) 893-0401');

insert into city (city_name,city_description,contact_title,contact_name,contact_email,contact_phone)
values ('Manti City','','City Recorder','JoAnn Otten', 'mantiadmin@mail.manti.com','435-835-2401');
insert into city (city_name,city_description,contact_title,contact_name,contact_email,contact_phone)
values ('Ephraim City','','City Recorder','Leigh Ann Warnock', 'leighann.warnock@ephraimcity.org','435-283-4631');

insert into state (state_name,state_description,contact_title,contact_name,contact_email,contact_phone)
values ('Utah','Utah Lieutenant Governor’s Office','Lieutenant Governor','DEIDRE HENDERSON','dhenderson@le.utah.gov','801-538-1041');

Insert into office
( office_name , county_id , city_id , state_id , positions_num )
Values
('Senate - District1',null,null,1,1),
('Senate - District2',null,null,1,1),
('Senate - District3',null,null,1,1),
('Mayor',null,1,null,1),
('City Council Member',null,1,null,3),
('Treasurer',null,2,null,1);

insert into candidate (candidate_name, candidate_email, candidate_phone)
values
('Bob Barker','Bob.Barker@nowhere.com','6191632151'),
('Betty Davis','Betty.Davis@nowhere.com','9839784277'),
('Cindy Curtain','Cindy.Curtain@nowhere.com','5907941246'),
('Sue Lawlerly','Sue.Lawlerly@nowhere.com','4153443791'),
('Bill Blenton','Bill.Blenton@nowhere.com','9776978537'),
('Hilton Honorton','Hilton.Honorton@nowhere.com','345121085'),
('Sally Salamander','Sally.Salamander@nowhere.com','8976797431'),
('Phil Pendelton','Phil.Pendelton@nowhere.com','1104076270'),
('Darlene Dearthandrol','Darlene.Dearthandrol@nowhere.com','8203793415'),
('Andy Albertson','Andy.Albertson@nowhere.com','3453074118'),
('Brenda Barbaduke','Brenda.Barbaduke@nowhere.com','5108953002'),
('Caleb Cerrara','Caleb.Cerrara@nowhere.com','5251922427'),
('Daniel Downotto','Daniel.Downotto@nowhere.com','1524706053'),
('Edwin Ernasto','Edwin.Ernasto@nowhere.com','5679922491'),
('Felicity Fabalesta','Felicity.Fabalesta@nowhere.com','7287833898'),
('Gene Gervasialo','Gene.Gervasialo@nowhere.com','9328039929'),
('Henrietta Helvetica','Henrietta.Helvetica@nowhere.com','2242862996'),
('Ivy Ignassio','Ivy.Ignassio@nowhere.com','1445683825'),
('Julie Jasmantal','Julie.Jasmantal@nowhere.com','9421477036'),
('Kilimanjaro Kerstakan','Kilimanjaro.Kerstakan@nowhere.com','6722215169'),
('Lucy Lundstrom','Lucy.Lundstrom@nowhere.com','8678076675'),
('Marry Margarettly','Marry.Margarettly@nowhere.com','2124940944'),
('Nancy Novemberstein','Nancy.Novemberstein@nowhere.com','4439122767'),
('Zaldana Zoolander','Zaldana.Zoolander@nowhere.com','5540077824'),
('Yiogio Yuluus','Yiogio.Yuluus@nowhere.com','6603989748'),
('Xiana Xysterston','Xiana.Xysterston@nowhere.com','2626455546'),
('William Whynotta','William.Whynotta@nowhere.com','1945445163'),
('Valerie Vianacious','Valerie.Vianacious@nowhere.com','5323914500')
;



insert into candidate_office (candidate_id, office_id, eliminated_tf, filed_date )
values
(1,1,false,now()),
(2,1,false,now()),
(3,1,false,now()),
(4,2,false,now()),
(5,2,false,now()),
(6,2,false,now()),
(7,3,false,now()),
(8,3,false,now()),
(9,3,false,now()),
(10,4,false,now()),
(11,4,false,now()),
(12,4,false,now()),
(13,4,false,now()),
(14,5,false,now()),
(15,5,false,now()),
(16,5,false,now()),
(17,5,false,now()),
(18,5,false,now()),
(19,5,false,now()),
(20,5,false,now()),
(21,5,false,now()),
(22,5,false,now()),
(23,5,false,now()),
(24,6,false,now()),
(25,6,false,now()),
(26,6,false,now()),
(27,6,false,now()),
(28,6,false,now());




--Display_The_Ballot_Choices
select
coalesce (ci.city_name,cty.county_name,s.state_name) juristiction,
o.office_name office,
o.positions_num NumberToElect,
c.candidate_name CandidateName,
c.candidate_email CandidateEmail,
co.filed_date filed_date
from 
city ci right outer join
office o on (o.city_id =ci.id) left outer join 
county cty on (cty.id=o.county_id) left outer join 
state s on (s.id=o.state_id) inner join 
candidate_office co on (co.office_id=o.id) inner join 
candidate c on (c.id=co.candidate_id)

