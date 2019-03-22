import psycopg2
import pandas as pd
import gpxpy.geo
import googlemaps
import pickle
from sqlalchemy import create_engine
import numpy as np
import os, glob
import time
import datetime

# setting up connections
gmaps = googlemaps.Client(key=open('key.txt','r'))
conn = psycopg2.connect("dbname='triplabmtl' user='postgres' host='localhost' password= 'postgres' ")
cur = conn.cursor()

def input_extract(user, trip_id):
    """Assumes that user is a string which is the user_id of traveller and ipere is an integer which is
    the unique trip id
    Returns a dataframe in which the possible bus routes within 15 meters are determined in the column
     called routelist"""

    sql_temp = """
        SELECT uuid,trip_id, latitude, longitude, northing, easting, timestamp,segment_group, break_period,
         speed, v_accuracy, h_accuracy, point_type, geom from coordinates_table where coordinates_table.uuid = '{}'
and trip_id = '{}' ORDER BY uuid, trip_id, timestamp;""".format(str(user), trip_id)
    cur.execute(sql_temp)
    conn.commit()

    # global ct
    ct = cur.fetchall()

    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])

    df = pd.DataFrame(ct, index=list(range(len(ct))), columns=col_names)

    df['Time_Difference'] = df['timestamp'] - df['timestamp'].shift(+1)

    df['Distance_Between'] = None
    df['Average_Speed'] = None

    for i in range(len(df) - 1):
        lon_1 = df.loc[i, "longitude"]
        lon_2 = df.loc[i + 1, "longitude"]
        lat_1 = df.loc[i, 'latitude']
        lat_2 = df.loc[i + 1, 'latitude']
        dist = gpxpy.geo.haversine_distance(lat_1, lon_1, lat_2, lon_2)
        df.set_value(i, 'Distance_Between', dist)

    # to make it compatible to postgres
    for i in range(len(df)):
        try:
            df.set_value(i, 'Time_Difference', df.loc[i, 'Time_Difference']).total_seconds()
        except:
            pass

    for i in range(len(df) - 1):
        time_seconds = (df.loc[i, 'Time_Difference']).total_seconds()
        speed = (df.loc[i, 'Distance_Between'] / time_seconds)
        if pd.notnull(speed):
            df.set_value(i, 'Average_Speed', speed)

    return df


def elevation_query(df):
    df['elevation'] = None
    for i in range(len(df)):
        lat = df.loc[i, 'latitude']
        lon = df.loc[i, "longitude"]
        elev_list = gmaps.elevation((lat, lon))
        elev_dic = elev_list[0]
        elevation = elev_dic['elevation']
        df.set_value(i, 'elevation', elevation)
    return df


def slope(df):
    ###determining slope
    for i in range(len(df) - 1):
        elev1 = df.loc[i, 'elevation']
        elev2 = df.loc[i + 1, 'elevation']
        delta_elev = elev2 - elev1
        if df.loc[i, "Distance_Between"] != 0:
            slope = delta_elev / df.loc[i, "Distance_Between"]
            df.set_value(i, 'slope', slope)
        else:
            pass
    return df


def places_query(df):
    df['places_type'] = None
    df['rating'] = None
    df['num_places'] = None
    for i in range(0, len(df), 4):
        places_all_dict = gmaps.places_nearby({u'lat': df.loc[i, 'latitude'], u'lng': df.loc[i, 'longitude']},
                                              radius=50)
        # 50 is a hyper parameter that can be learned later, I chose due to lack of sufficient quotas
        time.sleep(1)
        places_results = places_all_dict['results']
        num_places = len(places_results)
        types = []
        ratings = []
        for place in places_results:
            types.extend(place['types'])
            if 'rating' in place.keys():  # I somehow lost some part of the data, if this was not okay, I will add them
                ratings.append(place['rating'])

        df.set_value(i, 'places_type', types)
        df.set_value(i, 'rating', ratings)
        df.set_value(i, 'num_places', num_places)
    return df


def pickle_loader(foldername=""):
    os.chdir(
        "C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/" + foldername + "/")
    pickle_names = []
    for file in glob.glob("*.pkl"):
        pickle_names.append(file)
    return pickle_names


def df_to_sql(name, df):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/triplabmtl')
    df.to_sql(str(name), engine)


def pedestrian_pickler():
    os.chdir(
        "C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/coordinates/revealed/coordinates")
    pickle_names = []
    ## the upper procedure will be included in the fors below:
    for index, id_user_trip in df_keys.iterrows():
        pickle_name = 'pedestrian_coordinates_' + str(index) + ".pkl"
        for file in glob.glob("*.pkl"):
            pickle_names.append(file)
        if pickle_name not in pickle_names:
            df = input_extract(id_user_trip.uuid, id_user_trip.trip_id)
            df['trip_index'] = index
            df = elevation_query(df)
            df = slope(df)
            time.sleep(1)
            # df = places_query(df)   #reached the quota
            print(index, datetime.datetime.now())
            df.to_pickle(pickle_name)
            # df_final = pd.concat([df_final, df], ignore_index=True)



# getting the key for trip_id user from database
sql_temp = "SELECT * FROM trip_keys_final"
cur.execute(sql_temp)
col_names = []
for elt in cur.description:
    col_names.append(elt[0])

df_keys = pd.DataFrame(cur.fetchall(), columns=col_names)
df_to_sql('coordinates_pedestrian',df_final)

df_final = pd.DataFrame()
def reward_extractor(user, trip_id):
    tables_to_drop = ['revealed_temp', 'choice_line_temp']

    for table in tables_to_drop:
        sql_temp = """DROP TABLE IF EXISTS {}""".format(str(table))
        cur.execute(sql_temp)
        conn.commit()

    sql_temp = """create TABLE revealed_temp as select * from revealed_matched_lines_over250m WHERE uuid = '{}' and trip_id = {}""".format(str(user), trip_id)
    cur.execute(sql_temp)
    conn.commit()

    sql_temp = """create TABLE choice_line_temp as SELECT * from n_choice_polylines WHERE uuid = '{}' and trip_id = {}""".format(str(user),trip_id)
    cur.execute(sql_temp)
    conn.commit()

    sql_temp = """SELECT choice_line_temp.uuid, choice_line_temp.trip_id, choice_line_temp.alternative_no ,st_length(st_intersection(choice_line_temp.buffer_srid, revealed_temp.matched_geom)) as intersecion_length, st_length(revealed_temp.matched_geom) as line_length from choice_line_temp, revealed_temp  where revealed_temp.uuid = choice_line_temp.uuid and revealed_temp.trip_id= choice_line_temp.trip_id ;
"""
    cur.execute(sql_temp)
    conn.commit()
    ct = cur.fetchall()

    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])
    sub_df = pd.DataFrame(ct, index=list(range(len(ct))), columns=col_names)
    sub_df['overlap_percentage'] = sub_df['intersecion_length'] / sub_df['line_length']
    # for index, row in sub_df.iterrows():
    #     buffer = sub_df.loc[index, 'buffers']
    #     sql_temp = """SELECT st_intersection({},{}) from choice_line_srid,revealed_lines """.format(str(buffer),str(revealed_linegeom_str) )
    #     cur.execute(sql_temp)
    #     conn.commit()
    #     ct = cur.fetchall()

    return sub_df

# elastic_weights = pd.read_csv('C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/elastic_net_weights.csv')
# elastic_weights.drop('length', axis = 1, inplace = True)
# elastic_weights.set_index('name', inplace = True)
# elastic_weight_dict = elastic_weights.to_dict()




for index, id_user_trip in df_keys.iterrows():
    df = reward_extractor(id_user_trip.uuid, id_user_trip.trip_id)
    df_final = pd.concat([df_final, df], ignore_index=True)


\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00\00
