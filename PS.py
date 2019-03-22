import psycopg2
import pandas as pd
from shapely.geometry import LineString
import numpy as np


def df_to_sql(name, df):
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/triplabmtl')
    df.to_sql(str(name), engine)

def chopper(index):
    '''It gets an a trip index, and chops all of the alternatives into links in GIS'''


    sql_temp = '''DROP TABLE IF EXISTS lines_to_be_chopped_temp, line_to_be_chopped_temp_coll;
            SELECT trip_index, alternative_no, line_geom_srid INTO
            lines_to_be_chopped_temp FROM n_choice_polylines WHERE trip_index = {};
            SELECT trip_index, st_linemerge(ST_Collect(line_geom_srid)) INTO line_to_be_chopped_temp_coll FROM lines_to_be_chopped_temp GROUP BY trip_index;
                     '''.format(index)
    db_cur.execute(sql_temp)
    db_conn.commit()
    sql_temp = ''' SELECT json_build_object(
                     'type', 'Feature',
                     'geometry', ST_AsGeoJSON(st_linemerge)::json,'properties', json_build_object('type', 'type')) FROM line_to_be_chopped_temp_coll;'''
    db_cur.execute(sql_temp)
    geo_dict = db_cur.fetchone()[0]

    lines_from_json = []
    for item in geo_dict['geometry']['coordinates']:
        lines_from_json.append(item)
    print('len of lines_from_json is %s' % len(lines_from_json))
    new_links = []
    try:
        lines_from_json[0][0][0]
        for feature in lines_from_json:
            last_point = None
            for coordinate in feature:
                if last_point:
                    new_links.append((last_point, coordinate))
                last_point = coordinate
    except TypeError:
        df_lines_from_json = pd.DataFrame(lines_from_json)
        df_lines_from_json_shifted = df_lines_from_json.shift(-1)
        for i in range(len(df_lines_from_json)-1):
            new_links.append((list(df_lines_from_json.loc[i]),list(df_lines_from_json_shifted.loc[i])))

    print("number of lines in new_lines to create %s" % len(new_links))

    chopped_file_name = 'chopped_link_temp'
    print ("writing table %s" % chopped_file_name)

    sql_temp = """
        DROP TABLE IF EXISTS chopped_link_temp;
        CREATE TABLE chopped_link_temp (link_id INT , link_geom geometry);
        """
    db_cur.execute(sql_temp)
    db_conn.commit()

    counter = 0
    for item in new_links:
        link_id = counter

        wkt = LineString(item).wkt
        sql_temp = """
        INSERT INTO {chopped_file_name} (link_id, link_geom)
        --VALUES ( {link_id}, ST_SnapToGrid((ST_GeomFromText(('{line}'), {srid})),1)) ;
        VALUES ( {link_id}, (ST_GeomFromText(('{line}'), {srid})) ) ;
        """
        db_cur.execute(sql_temp.format(link_id=link_id, chopped_file_name = 'chopped_link_temp', line=wkt, srid=32618))
        db_conn.commit()
        counter += 1

    sql_temp = '''DELETE FROM chopped_link_temp ct WHERE EXISTS ( Select * from chopped_link_temp ctt 
      WHERE ctt.link_geom = ct.link_geom
      AND ct.link_id > ctt.link_id);'''
    db_cur.execute(sql_temp)
    db_conn.commit()

def ps_calculator():
    tables_to_drop = ['intersect_temp_count','link_COUNTER_TEMP', 'intersect_temp' ]
    for table in tables_to_drop:
        sql_temp = '''DROP TABLE IF EXISTS {};'''.format(table)
        db_cur.execute(sql_temp)
        db_conn.commit()
    sql_temp = '''SELECT a.trip_index , a.alternative_no,a.line_geom_srid ,b.link_id, b.link_geom, 
st_length(b.link_geom) as link_length ,st_length(a.line_geom_srid) as alt_length 
 INTO intersect_temp from lines_to_be_chopped_temp a, chopped_link_temp b
                WHERE st_intersects(st_buffer(a.line_geom_srid,0.5, 'endcap=flat'), b.link_geom);
                ALTER TABLE intersect_temp ADD COLUMN link_alt_ratio FLOAT;
                UPDATE intersect_temp  SET link_alt_ratio =link_length/alt_length;
                SELECT link_id, count(DISTINCT(alternative_no)) as number_of_paths_choice_set_sharing_link 
                INTO link_COUNTER_TEMP FROM intersect_temp GROUP By link_id;
                ALTER TABLE link_COUNTER_TEMP ADD COLUMN share_counter INT;
                UPDATE link_COUNTER_TEMP  SET share_counter =  number_of_paths_choice_set_sharing_link -1;
                SELECT intersect_temp.*,link_COUNTER_TEMP.share_counter INTO intersect_temp_count 
                FROM intersect_temp, link_COUNTER_TEMP WHERE intersect_temp.link_id = link_COUNTER_TEMP.link_id;
                ALTER TABLE intersect_temp_count ADD COLUMN link_ratio_divided_by_number_of_paths FLOAT;
                UPDATE intersect_temp_count  
                SET link_ratio_divided_by_number_of_paths =link_alt_ratio/share_counter WHERE share_counter!=0;
                '''
    db_cur.execute(sql_temp)
    db_conn.commit()

    # getting the intersect temp_count table into pandas dataframe to be manipulated

    #### ratio to be calculated in pandas!!

    df = pd.read_sql('SELECT trip_index, alternative_no, link_id,link_length,'
                     ' alt_length,link_alt_ratio, share_counter,link_ratio_divided_by_number_of_paths '
                     'FROM intersect_temp_count', db_conn)
    df_links_of_alternatives = pd.read_sql('SELECT  alternative_no ,array_agg(link_id) '
                            'FROM intersect_temp GROUP BY alternative_no;', db_conn, index_col ='alternative_no')
    df_links_of_alternatives.sort_index(inplace=True)

    df_alternative_link_dummy=pd.get_dummies(df_links_of_alternatives['array_agg'].apply(pd.Series).stack()).sum(level=0)
    df_alt_length = df[['alternative_no', 'alt_length']]
    df_alt_length.drop_duplicates(inplace=True)
    df_alt_length.set_index('alternative_no', inplace = True)

    ln_PS1= {}
    ln_PS2={}
    ln_PS3 = {}
    PS4 ={}

    for alternative, link_list in df_links_of_alternatives.iterrows():
        ps1_alt = 0
        ps2_alt = 0
        ps3_alt = 0
        ps4_alt = 0

        link_list = df_links_of_alternatives.loc[alternative].values[0]
        l_i = df_alt_length.loc[alternative,'alt_length']
        l_min = df_alt_length.min()[0]

        for link in link_list:
            l_a = df.loc[df['link_id'] == link].link_length.unique()[0]
            deltas = df_alternative_link_dummy[link]
            denom_ps1 = deltas.sum() ## this is for PS1

            ps1_alt += l_a/l_i * 1/denom_ps1 ## this is for PS1
            ps4_alt += l_a/l_i * np.log(1/denom_ps1) ## this is PS4
            denom2 = 0 ## this is for ln(PS2)
            denominator = 0 ## ## this is ln_PS3 Ramming 2002

            for alt_j, delta in deltas.iteritems():
                l_j = df_alt_length.loc[alt_j,'alt_length']
                denom2 += delta* (l_min/l_j)
                denominator += delta * (l_i/l_j)**14
            ps2_alt += l_a/l_i * 1/denom2
            ps3_alt = ps3_alt + l_a/l_i * 1/denominator

        ln_PS1[alternative] = np.log(ps1_alt)
        ln_PS2[alternative] = np.log(ps2_alt)
        ln_PS3[alternative] = np.log(ps3_alt)
        PS4[alternative] = ps4_alt

        df_ps1 = pd.DataFrame.from_dict(ln_PS1, orient='index').T
        df_ps2 = pd.DataFrame.from_dict(ln_PS2, orient='index').T
        df_ps3 = pd.DataFrame.from_dict(ln_PS3, orient='index').T
        df_ps4 = pd.DataFrame.from_dict(PS4, orient='index').T


    return df_ps1,df_ps2, df_ps3,df_ps4



if __name__=='__main__':
    db_conn = psycopg2.connect(dbname='triplabmtl', user='postgres', host='localhost', password='postgres')
    db_cur = db_conn.cursor()
    pd_indices = pd.read_sql('SELECT DISTINCT trip_index FROM n_trips_visual_processed ORDER BY trip_index;', db_conn)

    df_ps1 = pd.DataFrame(columns=['trip_index',0,1,2])
    df_ps2 = pd.DataFrame(columns=['trip_index',0,1,2])
    df_ps3 = pd.DataFrame(columns=['trip_index',0,1,2])
    df_ps4 = pd.DataFrame(columns=['trip_index',0,1,2])


    for _, trip_index_df in pd_indices.trip_index.iteritems():
        index = trip_index_df
        chopper(index)
        df_ps1_temp, df_ps2_temp, df_ps3_temp, df_ps4_temp = ps_calculator()
        df_ps1_temp['trip_index'] = index
        df_ps2_temp['trip_index'] = index
        df_ps3_temp['trip_index'] = index
        df_ps4_temp['trip_index'] = index

        df_ps1 = pd.concat([df_ps1, df_ps1_temp])
        df_ps2 = pd.concat([df_ps2, df_ps2_temp])
        df_ps3 = pd.concat([df_ps3, df_ps3_temp])
        df_ps4 = pd.concat([df_ps4, df_ps4_temp])

    df_to_sql('PS_1',df_ps1)
    df_to_sql('PS2',df_ps2)
    df_to_sql('PS3',df_ps3)
    df_to_sql('PS4',df_ps4)






