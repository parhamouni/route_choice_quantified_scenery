--- The mode_activity_trip_cleaned_valid_btw_home_work_study is my trips_table
--- the segment_trip_id_valid_btw_home_work_study is the coordinates_table
-- Trip_id conncets the two table, but it is not distinct!!
-- so I have to recreate the key myself
-- ALTER TABLE mode_activity_trip_cleaned_valid_btw_home_work_study
-- RENAME TO trips_table;
-- ALTER TABLE segment_trip_id_valid_btw_home_work_study
-- RENAME TO coordinates_table;

select * from trips_table where mode = 0 ; --- selecting pedestrian trips and finding out their trip_id and user_id
select uuid,trip_id, purpose from trips_table where mode = 0 ; --- selecting pedestrian trips and finding out their trip_id and user_id
select count(*) from (select uuid,trip_id from trips_table where mode = 0) as a; --- 1539 pedestrian trips
-----
select uuid,trip_id, purpose from trips_table where mode = 0 and purpose = 2 ; --- 124 trip with leisure purpose
select array_agg(distinct(purpose)), count(purpose) from trips_table where mode = 0 GROUP BY purpose order by purpose; --- 124 trip with leisure purpose, details are below
-- {0},554
-- {1},36
-- {2},124
-- {3},143
-- {4},25
-- {5},124
-- {6},254
-- {7},259
-- {8},20


--- WHAT IS THE PURPOSE OF 6 AND 7??

--- GENERATING KEY TABLE
select uuid,array_agg(distinct(trip_id))from trips_table where mode = 0 group by trips_table.uuid ; --- selecting pedestrian trips and their trip_ids
----
select uuid,count(distinct(trip_id))from trips_table where mode = 0 group by trips_table.uuid ; --- selecting pedestrian trips and their trip_ids
------
select count(*) from (select uuid,count(distinct(trip_id))from trips_table where mode = 0 group by trips_table.uuid) as a  where count >2 ; --- 122 users with more than 4 trips, 153 users with more than 3 trips,
---  THERE ARE 451 PEOPLE WITH PEDESTRIAN TRIPS IN THE DATA SET
SELECT count(*) from (select uuid,array_agg(distinct(trip_id)) from trips_table group by trips_table.uuid) as a;

--- cleaning the data of coordinates
-- ALTER TABLE coordinates_table
-- DROP COLUMN altitude;

---- Creating the coordinates table
SELECT uuid,trip_id, latitude, longitude, northing, easting, timestamp,segment_group, break_period, speed, v_accuracy, h_accuracy, point_type, geom from coordinates_table where coordinates_table.uuid = '17E9D0AB-1DF0-4EE9-AD90-630FB1095B37'
ORDER BY uuid, trip_id, timestamp;

Select count(*) from coordinates_table;
select count(*) from test_user;


---- creating the key
CREATE TABLE trip_keys as select trip_index,unnest(array_agg(DISTINCT  uuid))  as uuid, unnest(array_agg(DISTINCT trip_id)) as trip_id from coordinates_choice_set GROUP BY trip_index;

SELECT count(uid) from trips_table;


------------- creating pedestrian trips

CREATE TABLE mytable AS SELECT DISTINCT tk.trip_index, tt.*
FROM trips_table tt
INNER JOIN trips_keys tk
    ON tk.uuid = tt.uid
    AND tk.trip_id = tt.trip_id;

CREATE TABLE trips_pedestrian as SELECT DISTINCT on (mytable.trip_index) * from  mytable ORDER BY trip_index;

ALTER TABLE trips_pedestrian ADD PRIMARY KEY (trip_index);

--- finding out how to do line in postGIS



alter table coordinates_pedestrian_30 add column points_geom geometry;
UPDATE coordinates_pedestrian_30 SET points_geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude,latitude),4326), 32618);

SELECT count(*) FROM coordinates_table;

-- alter table test_user_final add column geom32618 geometry;
-- UPDATE test_user_final SET geom32618 = ST_Transform(ST_SetSRID(ST_MakePoint(longitude,public.test_user_final.latitude),4326), 32618);

SELECT coordinates_pedestrian_30.trip_index, ST_MakeLine(coordinates_pedestrian_30.points_geom ORDER BY timestamp)as linegeom into trip_index_line FROM coordinates_pedestrian_30 GROUP BY trip_index;
SELECT  gps.geom32618, ST_MakeLine(gps.geom32618 ORDER BY timestamp) as linegeom  FROM test_user_final As gps GROUP BY gps.geom32618;
SELECT  gps.geom32618, ST_MakeLine(gps.geom32618 ORDER BY index) as linegeom  FROM test_df As gps GROUP BY gps.geom32618;

SELECT * into trip_39 from trips_pedestrian where trip_index < 40;
alter table trip_39 add column linegeom geometry;
UPDATE trip_39 SET linegeom =ST_MakeLine(coordinates_pedestrian_30.points_geom)  FROM coordinates_pedestrian_30;



SELECT trip_index,uuid, trip_id, avg(sl)

SELECT trip_39.*, trip_index_line.linegeom
FROM trip_39  INNER JOIN trip_index_line on trip_39.trip_index = trip_index_line.trip_index;

CREATE TABLE trip_39_lines AS SELECT trip_39.*, trip_index_line.linegeom
FROM trip_39  INNER JOIN trip_index_line on trip_39.trip_index = trip_index_line.trip_index;

alter table trip_39 DROP column linegeom;
UPDATE test_df SET geom32618 = ST_Transform(ST_SetSRID(ST_MakePoint(lng,lat),4326), 32618);
SELECT  gps.geom32618, ST_MakeLine(gps.geom32618 ORDER BY index) as linegeom  FROM test_df As gps GROUP BY gps.geom32618;
create TABLE test_geom2 as (SELECT  gps.trip_id, ST_MakeLine(gps.geom32618 ORDER BY trip_id) as linegeom  FROM test_df As gps GROUP BY gps.trip_id); --- change gps.trip_id with the index of alternative*trip



CREATE TABLE test_real_user as SELECT uuid,trip_id, latitude, longitude, northing, easting, timestamp,segment_group, break_period, speed, v_accuracy, h_accuracy, point_type, geom from coordinates_table where coordinates_table.uuid = 'F537DFBD-4247-44C5-89AB-C54C3B42B9C4'
ORDER BY uuid, trip_id, timestamp;

-- select *  into test_postgis from test_user;
-- DROP table IF EXISTS test_postgis;
-- create TABLE test_geom2 as SELECT  gps.geom, ST_MakeLine(gps.geom ORDER BY TIMESTAMP) as linegeom  FROM test_postgis As gps GROUP BY gps.geom;
-- DROP table IF exists test_geom2;
-- SELECT * FROM test_user;
-- SELECT postgis_version();
-- CREATE EXTENSION postgis_topology;




---- Creating the Choice aggregate variables

create table trips_choice as SELECT ROW_NUMBER() OVER (ORDER BY trip_index) as choice_trip_index,trip_index ,alternative_no ,unnest(array_agg(distinct(distance_meter))) as distance,unnest(array_agg(distinct(duration_seconds))) as duration, avg(slope) as avg_slope , variance(slope) as var_slope,max(slope) as max_slope, min(slope) as min_slope,
unnest(array_agg(distinct(number_of_turns))) as number_of_turns FROM coordinates_choice_set_final GROUP BY trip_index, alternative_no, distance_meter, duration_seconds, number_of_turns;

----

select trip_index, uid, trip_id, olat, olon, dlat, dlon from trips_pedestrian;

SELECT * FROM coordinates_choice_39 order by trip_index, alternative_no,index;

---- creating linegoem
select trip_index, alternative_no, ST_LineFromEncodedPolyline(unnest(array_agg(distinct(polyline)))) into trips_choice_39 from coordinates_choice_39  GROUP BY  trip_index, alternative_no;


select trip_index, alternative_no, ST_LineFromEncodedPolyline(unnest(array_agg(distinct(polyline)))) into trips_choice_39 from coordinates_choice_39  GROUP BY  trip_index, alternative_no;



--- creating the
select *  from trips_pedestrian where merge_codes like '';
select DISTINCT on (merge_codes) merge_codes from trips_table  where mode = 1 order by merge_codes;


SELECT * from trips_keys WHERE trip_index != 26 AND trip_index != 28 and trip_index != 27 and trip_index != 33 and trip_index != 32 and trip_index != 36 and trip_index != 39;

SELECT *, ROW_NUMBER() OVER (ORDER BY trip_index) as trip_ids into trips_pedestrian_modified FROM trips_pedestrian where cumulative_distance < 2500 and travel_time> 10 and travel_time< 60; ---- based on Probability distribution of walking trips and effects of restricting free pedestrian movement on walking distance Alejandro Tirachini
SELECT *  FROM trips_pedestrian where cumulative_distance < 2500 and travel_time> 10 and travel_time< 60; ---- based on Probability distribution of walking trips and effects of restricting free pedestrian movement on walking distance Alejandro Tirachini

SELECT *, ROW_NUMBER() OVER (ORDER BY trip_index) as trip_ids into trips_pedestrian_modified FROM  trips_revealed_modified order by  trip_index;
drop table if EXISTS trips_revealed_modified;

SELECT * FROM trips_revealed_modified;
alter table trips_revealed_modified DROP COLUMN trip_index;
alter TABLE trips_revealed_modified rename trip_ids TO trip_index;

CREATE INDEX  ON trips_revealed_modified (trip_index);



----- my final coordinates_choice is coordinates_choice_Set_final

ALTER TABLE coordinates_choice_set_final DROP COLUMN index;
ALTER TABLE coordinates_choice_set_final RENAME level_0 to index;

SELECT * FROM coordinates_choice_set_final where trip_index = 11;

SELECT count(*) FROM coordinates_choice_set_final;



---tables to work with
-- want to create choice trips with
SELECT ROW_NUMBER() OVER (ORDER BY trip_index) as choice_trip_index,trip_index ,alternative_no ,unnest(array_agg(distinct(distance_meter))) as distance,unnest(array_agg(distinct(duration_seconds))) as duration, avg(slope) as avg_slope , variance(slope) as var_slope,max(slope) as max_slope, min(slope) as min_slope, ST_LineFromEncodedPolyline(unnest(array_agg(distinct(polyline)))) as linegeom,
unnest(array_agg(distinct(number_of_turns))) as number_of_turns FROM coordinates_choice_set_final GROUP BY trip_index, alternative_no, distance_meter, duration_seconds, number_of_turns;
--- coordionates_pedestrian places and scenes

SELECT count(*) from coordinates_places where num_places is not null;

SELECT distinct(trip_index,uuid, trip_id),trip_index, trip_id, ROW_NUMBER() OVER (ORDER BY trip_index) as index into trips_keys FROM coordinates_choice_set_final  group by trip_index order by trip_index;

SELECT DISTINCT (trip_index), uuid, trip_id into trip_keys from coordinates_choice_set_final;

alter TABLE trip_keys drop COLUMN index ;
SELECT *,ROW_NUMBER() OVER (ORDER BY trip_index) as revealed_index into trip_keys_final
FROM trip_keys;

alter table trip_keys_final RENAME COLUMN trip_index to choice_trip_index;

CREATE INDEX index_final on  trip_keys();
alter TABLE trip_keys drop column index_final;
CREATE INDEX  ON trip_keys (index_final);

SELECT *
FROM trip_keys order by trip_index;
CREATE INDEX  ON trip_keys (trip_index);


SELECT (max(trip_index)) from coordinates_revealed_elevation;

SELECT * FROM coordinates_choice_set_final where uuid = '04322784-A2AD-44E8-B415-A7B0FAB42C59' and trip_id = 2;

SELECT polyline,unnest(array_agg(DISTINCT  alternative_no)), unnest(array_agg(DISTINCT  uuid)),unnest(array_agg(DISTINCT  trip_id)) , unnest(array_agg(DISTINCT  trip_index)) from coordinates_choice_set_final GROUP BY polyline;

select trip_index ,trip_id,uuid, alternative_no, ST_LineFromEncodedPolyline(unnest(array_agg(distinct(polyline)))) as choice_line_geom into choice_line from coordinates_choice_set_final  GROUP BY  trip_index, trip_id,uuid, trip_index, alternative_no;

SELECT *
FROM ;
select trip_index, ST_MakeLine(geom order by trip_index ,TIMESTAMP) as linegeom  from coordinates_revealed_elevation  GROUP BY trip_index;

SELECT  gps.geom32618, ST_MakeLine(gps.geom32618 ORDER BY index) as linegeom  FROM test_df As gps GROUP BY gps.geom326;
SELECT revealed_index from trip_keys_final WHERE uuid = '2716a43e-4d96-4c41-b235-3d76d01d04da' and trip_id = 167;

create table choice_line_buffer as SELECT *,st_buffer(choice_line_geom,10) as buffers FROM choice_line;

---revealed_linegeom_str = '01020000206A7F00000F000000585BE52E6BAC22411D642B44763A5341B576757DD1AC2241C5BF07D4783A53418306103D38AD22410CD11264793A53414EA393909BAD22418A359F077C3A5341F29AEFB1CAAD22411B72396A873A5341B858234E03AE2241BAF03712923A5341A312F2EB3AAE2241C1563A1D9D3A53411EA9B39F6EAE2241633E3B34A83A5341D49C2AD4A1AE224116D8BE7CB33A5341464C985805AF224136B5B77CB13A53410C136FAE52AF2241D4085329B83A5341684AABBD80AF2241F6E89559C33A534140E3B7F6B7AF2241A00E2ED6CD3A5341CC51FEFFE4AF22418B09D885D83A53413AE30AD43CB022413E442306D33A5341'


---- buffer =  '0103000020E610000001000000780000006C8D249CA8E154C0A2C76BA6DA0E47402F150CBBBAE154C0274F17C9273C474032D6A26682D754C0D3DB57D482A247401E61CE2EA7D254C06E3048DA29E94740B2671F7D73CD54C0CEA48A113F074840B7FB8127ADC854C0DA8775FB1037484061F9F860CFB454C063D28001BF954840474B7C78A5A954C03444DA594ED648400236815D8AA854C0F5B3D69738D0484022C87AED2B9754C08C806AA1FF2249401C0CFE71314F54C0128DD35486F64940637EF7811A4F54C0433ECD0DBDF649408D0FB0E7FF3154C03A305E4B07334A408BCC6A51213354C06FDC3DF430354A407D65EC1B1A3354C0D356000740354A406DC6E104642654C0228EC2AE134B4A40875FDAFCC81054C0FE1D1473D5774A40D5306C59C71054C03EA50D66D8774A4046587947E1EF53C003BCCA9CADA84A408D5B262BE9D353C000415A5DB4D84A40196F4AE0B2C153C06EBD67133AED4A401C70AF6CF3AF53C02B2B0F1A92074B404BDC16609C8853C0FB359F728E2D4B400549FDC19A6653C0623CC762E0534B404119CFEBE95453C0A9B7D14B795F4B404EE6D5BF5C4353C00A5B86C96B704B40BA39FBC1741653C0B6B7701A6B884B40B4E29E3962EF52C0C621425508A24B4096D904A831DF52C084E9C423F3A54B402B47311DDECE52C092A8B3A2ACAE4B407D10C39F979D52C036FB0492D2B54B4040A71A75D47252C01851E14F2BC04B404E8D5C68E46452C0D70B773C0CBE4B401234A8279A5652C0F666B5EC1EC04B403BC92C5C6C2252C0F722034EEEB34B40195298CEBAF551C018BA2BD720AD4B40B7C4298387EA51C0CC1D7A80DFA64B40478D6FC6D5DE51C0E9BC7C1C24A44B40BDFDE65966A951C056D031CE7F824B40EF0C6800E47C51C0ED0CDF3DA4694B40E37C0FAFA07451C0AF490A6046614B405D1C5E59D16B51C0E5678D75BA5B4B40FC09CADECD3651C0108772ECAC224B40491330D9F40C51C0AA0810724DF84A403420E0658B0751C0698DBDA0D0EF4A4066E86716A20151C05C9C630374E94A4031A79398A10151C015F09D5B73E94A40DF003CA4440251C003E4A4388AE74A4039C3838C3AAA50C0FCFA7679775D4A409149AD633BA150C0E8D56F4C82484A40361F2AA168A050C0BCEA0469284A4A40B3EAC6CFF39F50C0DA57211A3F494A40F0CCAE1A0A8150C001DFE85884FD494024CE5661805850C01EE7EA97159F4940BBB5B387115450C03BD95233598F4940C3C49342FB4D50C0B25888CC6F804940F0B1A9E3A63550C0CC5C2CDB602349407250A05EEA1A50C00EC298C578C4484036BB4C12AF1750C0E0F694DCC0B048403C40DCF2EB1150C08DBA35A7B69A4840AB71BC0E0C0350C006304097D532484080BBC5CAACE74FC06AF5DBB407D64740D4007175DDE44FC075C7F8BDCDBE474058ACEB0979DC4FC03566D3CF7FA14740B0D6ED253AD44FC0B11FD98B463547402E8C26D089C94FC0837ED32BECDC46401F6E6F0784CB4FC0D37B896AF0C24640ED3B015AC7C84FC04A060115049F46408F16661759D64FC01F69F2F3993446402D23DC4894DC4FC035D4CEDEB8E24540106CBF8E53E44FC032CE0787FAC64540947FDBA091E94FC0832F3F99DD9D454060C55838BD0550C003414A5CC53A45402C681471081050C0E0496E420BF14440A3C72169311750C0A9D26F43C2D44440A32D50C8BF1E50C0A308E47F98A84440D56AB151C71E50C0C06248BB77A84440A2BED8DAB93950C0532E27CD564C44404DEAFBD6B34850C09556FEF32C1144402E9C0B06DF5150C06A75916DCAF943403E591524215E50C0C8B0EFBBE1CF4340C4B05A98148250C08861E613D57E4340257148D31E9650C073B6A55AB84B434033D8C608269650C00F3CE347A94B434015A83238929750C0485AA6FB614E43406EA4A4D463B150C09C2903FF2C144340AE5DCF465C1551C0B7ED4A28907C42405F3CBAF9328651C0F08804C6DE0E4240DD53E3D391FF51C02D9C0EFF4FCF4140D10839CCCE7C52C07550481A55C041408A49F2CC19F952C05C47227781E241406B53BB0CAC6F53C00973C6E38434424088BB04548E7653C00C6B1CBD923C42402A09B02C337653C099C4E32C9D3A42405CEFEADE84D053C0AD3D940DFEA24240435F2D0087D053C0EDC48D0001A342405FB83516C3D953C0A67420B4A4B0424066AC670CF7DB53C0D74F8E8838B34240D0BEFF633DDE53C09963B6C441B7424086EACE18FAEF53C0ECDADC4974D142403E7DEC337BF053C007793B733FD24240DC01FF6BB81354C09ADC4E6B25164340FCEFDD6AD13954C039E6ACE9BD594340C2572940FE4154C0F7B65CCF4D6F4340A648A3229D4C54C0405CA885C48343405AB025386A6954C02A52F99D47D74340EF0D3AD69F8554C0B85802CEAE21444077D2EB7FB58B54C068605F23B83A4440572E997A409554C00D4081D563564440F2C3AA90A2A954C0C9C67836DBB5444053405D8778BC54C01A16D8335C0345400BCCDADE18C054C08C2DD4490F1F4540E6CC3A347EC754C015AF18E3B2414540A26D141719D254C09B0BA5E98EA84540B436D8EA3FDC54C015452AE819F64540A15EFF7A21DD54C041607A08911346408221506254E154C0EB1CD4A84A3C4640EA1378757EE154C0046308427AA54640915C3B5DBDE354C0FEAED5D893F046406C8D249CA8E154C0A2C76BA6DA0E4740'

SELECT st_intersection(choice_line_buffer.buffers, revealed_lines.linegeom) from choice_line_buffer,revealed_lines;

alter table choice_line_buffer add column buffer_srid geometry;
UPDATE choice_line_buffer SET buffer_srid = ST_Transform(ST_SetSRID(buffers,4326), 32618);


alter table revealed_lines add COLUMN linegeom_srid geometry;

UPDATE revealed_lines SET linegeom_srid = ST_Transform(ST_SetSRID(linegeom,4326), 32618);

alter table coordinates_revealed_elevation add column points_geom geometry;
UPDATE coordinates_revealed_elevation SET points_geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude,latitude),4326), 32618);


create table revealed_lines_coorected_geom as select trip_index, ST_MakeLine(points_geom order by trip_index ,TIMESTAMP) as linegeom  from coordinates_revealed_elevation  GROUP BY trip_index;

SELECT st_intersection(choice_line_buffer.buffer_srid, revealed_lines_coorected_geom.linegeom) from choice_line_buffer,revealed_lines_coorected_geom where choice_line_buffer.trip_index = revealed_lines_coorected_geom.trip_index;


select trip_index ,trip_id,uuid, alternative_no,  ST_Transform(ST_SetSRID(ST_LineFromEncodedPolyline(unnest(array_agg(distinct(polyline)))),4326), 32618) as choice_line_geom_srid into choice_line_srid from coordinates_choice_set_final  GROUP BY  trip_index, trip_id,uuid, trip_index, alternative_no;

alter table choice_line_srid add column buffer_srid geometry;

UPDATE choice_line_srid SET buffer_srid = st_buffer(choice_line_geom_srid,20);

create table revealed_lines as SELECT revealed_lines_coorected_geom.* , trip_keys_final.uuid, trip_keys_final.trip_id
FROM  revealed_lines_coorected_geom, trip_keys_final where trip_keys_final.revealed_index = revealed_lines_coorected_geom.trip_index;

CREATE index on revealed_lines (trip_index);



SELECT choice_line_srid.trip_index , choice_line_srid.alternative_no ,st_length(st_intersection(choice_line_srid.buffer_srid, revealed_lines.linegeom)) from choice_line_srid, revealed_lines  where revealed_lines.trip_index = choice_line_srid.trip_index;

create TABLE revealed_temp as select * from revealed_lines where trip_index = 1;

create TABLE choice_line_temp as select * from choice_line_srid where trip_index = 1;
SELECT * from revealed_lines
inner join revealed_lines on  revealed_lines.trip_index = choice_line_srid.trip_index;

SELECT choice_line_temp.trip_index as choice_trip_index, choice_line_temp.uuid, choice_line_temp.trip_id, choice_line_temp.alternative_no ,st_length(st_intersection(choice_line_temp.buffer_srid, revealed_temp.linegeom)) as intersecion_length, st_length(revealed_temp.linegeom) as line_length from choice_line_temp, revealed_temp  where revealed_temp.uuid = choice_line_temp.uuid;


----
SELECT * from coordinates_table where uuid = '8dd3b757-1a41-4884-b589-4bf3d0c0bcc9' and trip_id = 69 order by timestamp

drop table if exists  choice_test;
---- demo
SELECT * into choice_test from choice_line_srid where uuid = 'E57CBC05-41A9-4E99-9848-706B89867969' and trip_id = 38;
SELECT * into point_test from coordinates_table where uuid = 'E57CBC05-41A9-4E99-9848-706B89867969' and trip_id = 38;
SELECT * into revealed_test from revealed_lines_srid where uuid = 'E57CBC05-41A9-4E99-9848-706B89867969' and trip_id = 38;

SELECT * from overlap_percentages where uuid = 'E57CBC05-41A9-4E99-9848-706B89867969' and trip_id = 38;
drop table if exists  revealed_test;


create table revealed_lines_srid as select uuid,trip_id,trip_index,  ST_MakeLine(points_geom order by TIMESTAMP) as linegeom  from coordinates_revealed_elevation  GROUP BY uuid,trip_id,trip_index;
create table trips_choice_places as SELECT trip_id, uuid, trip_index, alternative_no, avg(num_places) as avg_num_places, variance(num_places) as var_num_places ,sum(tags_accounting) as sum_accout, sum(tags_art_gallery) as sum_tags_art_gallery, sum(
    tags_atm) as sum_tags_atm
, sum(tags_bakery) as sum_tags_bakery, sum(tags_bank) as sum_tags_bank, sum(tags_bar) as sum_tags_bar, sum(tags_beauty_salon) as sum_tags_beauty_salon, sum(tags_bicycle_store) as sum_tags_bicycle_store
, sum(tags_book_store) as sum_tags_book_store
, sum(tags_bowling_alley) as sum_tags_bowling_alley
, sum(tags_bus_station) as sum_tags_bus_station
, sum(tags_cafe) as sum_tags_cafe
, sum(tags_car_dealer) as sum_tags_car_dealer
, sum(tags_car_rental) as sum_tags_car_rental
, sum(tags_car_repair) as sum_tags_car_repair
, sum(tags_car_wash) as sum_tags_car_wash
, sum(tags_cemetery) as sum_tags_cemetery
, sum(tags_church) as sum_tags_church
, sum(tags_city_hall) as sum_tags_city_hall
, sum(tags_clothing_store) as sum_tags_clothing_store
, sum(tags_convenience_store) as sum_tags_convenience_store
, sum(tags_courthouse) as sum_tags_courthouse
, sum(tags_dentist) as sum_tags_dentist
, sum(tags_department_store) as sum_tags_department_store
, sum(tags_doctor) as sum_tags_doctor
, sum(tags_electrician) as sum_tags_electrician
, sum(tags_electronics_store) as sum_tags_electronics_store
, sum(tags_embassy) as sum_tags_embassy
, sum(tags_establishment) as sum_tags_establishment
, sum(tags_finance) as sum_tags_finance
, sum(tags_fire_station) as sum_tags_fire_station
, sum(tags_florist) as sum_tags_florist
, sum(tags_food) as sum_tags_food
, sum(tags_funeral_home) as sum_tags_funeral_home
, sum(tags_furniture_store) as sum_tags_furniture_store
, sum(tags_gas_station) as sum_tags_gas_station
, sum(tags_general_contractor) as sum_tags_general_contractor
, sum(tags_grocery_or_supermarket) as sum_tags_grocery_or_supermarket
, sum(tags_gym) as sum_tags_gym
, sum(tags_hair_care) as sum_tags_hair_care
, sum(tags_hardware_store) as sum_tags_hardware_store
, sum(tags_health) as sum_tags_health
, sum(tags_home_goods_store) as sum_tags_home_goods_store
, sum(tags_hospital) as sum_tags_hospital
, sum(tags_insurance_agency) as sum_tags_insurance_agency
, sum(tags_jewelry_store) as sum_tags_jewelry_store
, sum(tags_laundry) as sum_tags_laundry
, sum(tags_lawyer) as sum_tags_lawyer
, sum(tags_library) as sum_tags_library
, sum(tags_liquor_store) as sum_tags_liquor_store
, sum(tags_local_government_office) as sum_tags_local_government_office
, sum(tags_locality) as sum_tags_locality
, sum(tags_locksmith) as sum_tags_locksmith
, sum(tags_lodging) as sum_tags_lodging
, sum(tags_meal_delivery) as sum_tags_meal_delivery
, sum(tags_meal_takeaway) as sum_tags_meal_takeaway
, sum(tags_mosque) as sum_tags_mosque
, sum(tags_movie_rental) as sum_tags_movie_rental
, sum(tags_movie_theater) as sum_tags_movie_theater
, sum(tags_moving_company) as sum_tags_moving_company
, sum(tags_museum) as sum_tags_museum
, sum(tags_neighborhood) as sum_tags_neighborhood
, sum(tags_night_club) as sum_tags_night_club
, sum(tags_painter) as sum_tags_painter
, sum(tags_park) as sum_tags_park
, sum(tags_parking) as sum_tags_parking
, sum(tags_pet_store) as sum_tags_pet_store
, sum(tags_pharmacy) as sum_tags_pharmacy
, sum(tags_physiotherapist) as sum_tags_physiotherapist
, sum(tags_place_of_worship) as sum_tags_place_of_worship
, sum(tags_plumber) as sum_tags_plumber
, sum(tags_point_of_interest) as sum_tags_point_of_interest
, sum(tags_police) as sum_tags_police
, sum(tags_political) as sum_tags_political
, sum(tags_post_office) as sum_tags_post_office
, sum(tags_premise) as sum_tags_premise
, sum(tags_real_estate_agency) as sum_tags_real_estate_agency
, sum(tags_restaurant) as sum_tags_restaurant
, sum(tags_roofing_contractor) as sum_tags_roofing_contractor
, sum(tags_route) as sum_tags_route
, sum(tags_school) as sum_tags_school
, sum(tags_shoe_store) as sum_tags_shoe_store
, sum(tags_shopping_mall) as sum_tags_shopping_mall
, sum(tags_spa) as sum_tags_spa
, sum(tags_stadium) as sum_tags_stadium
, sum(tags_storage) as sum_tags_storage
, sum(tags_store) as sum_tags_store
, sum(tags_sublocality) as sum_tags_sublocality
, sum(tags_sublocality_level_1) as sum_tags_sublocality_level_1
, sum(tags_subway_station) as sum_tags_subway_station
, sum(tags_supermarket) as sum_tags_supermarket
, sum(tags_synagogue) as sum_tags_synagogue
, sum(tags_train_station) as sum_tags_train_station
, sum(tags_transit_station) as sum_tags_transit_station
, sum(tags_travel_agency) as sum_tags_travel_agency
, sum(tags_university) as sum_tags_university
, sum(tags_veterinary_care) as sum_tags_veterinary_care
,avg(rating_average) as avg_rating_avg
,variance(rating_average) as var_rating_average
,avg(scenic_index) as avg_scenic_index
,variance(scenic_index) as var_scenic_index
FROM coordinates_choice_set_processed group by trip_id, uuid, trip_index, alternative_no;


create table choice_trips_overlap as SELECT trips_choice_places.*, overlap_percentages.overlap_percentage, overlap_percentages.index from trips_choice_places, overlap_percentages where trips_choice_places.uuid = overlap_percentages.uuid and trips_choice_places.trip_id = overlap_percentages.trip_id and overlap_percentages.alternative_no = trips_choice_places.alternative_no;
CREATE TABLE processed_choice_set as SELECT choice_trips_overlap.*, trips_choice.distance, trips_choice.duration, trips_choice.avg_slope, trips_choice.var_slope, trips_choice.max_slope, trips_choice.min_slope,trips_choice.number_of_turns from trips_choice, choice_trips_overlap where choice_trips_overlap.index = trips_choice.choice_trip_index;
CREATE INDEX on processed_choice_set(index);

alter table processed_choice_set drop column sum_tags_political;

create table processed_choice_set_with_user_information as SELECT processed_choice_set.*, trips_revealed_modified.age_bracket, trips_revealed_modified.sex,trips_revealed_modified.purpose from trips_revealed_modified, processed_choice_set where trips_revealed_modified.uuid = processed_choice_set.uuid and trips_revealed_modified.trip_id = processed_choice_set.trip_id;










