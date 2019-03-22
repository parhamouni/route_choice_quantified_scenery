import googlemaps
import pandas as pd
import numpy as np
import psycopg2
import time
import glob, os
import datetime
import pickle
import gpxpy.geo



gmaps = googlemaps.Client(key=open('key.txt','r'))
try:
    conn = psycopg2.connect("dbname='triplabmtl' user='postgres' host='localhost' password= 'postgres' ")
except:
    print("I am unable to connect to the database")
cur = conn.cursor()



def pickle_loader(foldername = ""):
    os.chdir(
        "C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/" + foldername + "/")
    pickle_names = []
    for file in glob.glob("*.pkl"):
        pickle_names.append(file)

    df_final = pd.DataFrame()


    for index, pickler in enumerate(pickle_names):
        pickler = "C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/" + foldername +"/"+ pickler
        with (open(pickler, "rb")) as openfile:
            while True:
                try:
                    df = pickle.load(openfile)
                    # df['trip_index'] = index
                    df_final = pd.concat([df_final,df], ignore_index=True)

                except EOFError:
                    break
    df_final.columns = ['alternative_number', 'distance_meter', 'duration_seconds', 'elevation','latitude', 'longitude', 'number_of_turns', 'uuid', 'trip_id', 'trip_index']
    return df_final

####getting the alternatives

def trips_pedstrian():

    ### electing pedestrian trips and finding out their trip_id and user_id
    query = "select trip_index, uid, trip_id, olat, olon, dlat, dlon from trips_pedestrian_modified; --- selecting pedestrian trips and finding out their trip_id and user_id"
    cur.execute(query)

    ct = cur.fetchall()

    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])

    trips_pedestrian = pd.DataFrame(ct, index=list(range(len(ct))), columns=col_names)
    trips_pedestrian.set_index('trip_index', inplace=True)

    conn.commit()

    return trips_pedestrian



def choice_co_extractor_pickler(trips_pedestrian, foldername = ""):

    os.chdir(
        "C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/" + foldername + "/")
    pickle_names = []
    for file in glob.glob("*.pkl"):
        pickle_names.append(file)

    for trip_index,uuid_trip in trips_pedestrian.iterrows():
        pickle_name = str(trip_index) + '_choice_coordinates.pkl'
        if pickle_name in pickle_names:
            pass
        else:
            olat = uuid_trip.olat
            olon = uuid_trip.olon
            dlat = uuid_trip.dlat
            dlon = uuid_trip.dlon
            choice_set_pd = pd.DataFrame(
                gmaps.directions((olat, olon), (dlat, dlon), mode='walking', alternatives=True))
            choice_set_pd['alternative_no'] = np.arange(len(choice_set_pd))
            choice_set_pd.set_index('alternative_no', inplace=True)
            nom_alternatives = len(choice_set_pd)

            df_alternatives = pd.DataFrame()
            if nom_alternatives > 1:  # to check we have a choice set
                for alternative_no, poli in choice_set_pd.iterrows():
                    polyline = poli.overview_polyline['points']
                    coordinates_elevation_dic = gmaps.elevation_along_path(path=polyline, samples=5)
                    coordinates_elevation_pd = pd.DataFrame(coordinates_elevation_dic)
                    coordinates_elevation_pd['polyline'] = polyline
                    coordinates_elevation_pd['alternative_no'] = alternative_no
                    coordinates_elevation_pd['Distance_Between'] = None
                    coordinates_elevation_pd['slope'] = None

                    loc_pd_final = pd.DataFrame()
                    for index, sample_dic in coordinates_elevation_pd.location.iteritems():
                        loc_pd = pd.DataFrame.from_dict(sample_dic, orient='index')
                        loc_pd_T = loc_pd.T
                        loc_pd_final = pd.concat([loc_pd_final, loc_pd_T], ignore_index=True)

                    coordinates_elevation_pd = pd.concat([coordinates_elevation_pd, loc_pd_final], axis=1)
                    coordinates_elevation_pd = coordinates_elevation_pd.drop(columns=['location', 'resolution'])

                    dict_choice_leg = choice_set_pd.legs.iloc[alternative_no][
                        0]  # extracting the distance in meters for each alternative
                    distance = dict_choice_leg['distance']['value']
                    duration = dict_choice_leg['duration']['value']
                    number_of_maneuvers = len(dict_choice_leg['steps'])
                    coordinates_elevation_pd['distance_meter'] = distance
                    coordinates_elevation_pd['duration_seconds'] = duration
                    coordinates_elevation_pd['number_of_turns'] = number_of_maneuvers - 1
                    coordinates_elevation_pd = coordinates_elevation_pd.rename(columns={'lat': 'latitude','lng':'longitude'})
                    coordinates_elevation_pd = Distance_Between(coordinates_elevation_pd)
                    coordinates_elevation_pd = slope(coordinates_elevation_pd)
                    df_alternatives = pd.concat([df_alternatives, coordinates_elevation_pd], ignore_index=True)
                    time.sleep(1)

                df_alternatives['uuid'] = uuid_trip.uid
                df_alternatives['trip_id'] = uuid_trip.trip_id
                df_alternatives['trip_index'] = trip_index
                print(trip_index,datetime.datetime.now())
                df_alternatives.to_pickle(pickle_name)

def Distance_Between(df):
    for i in range(len(df)-1):
        lon_1 = df.loc[i, "longitude"]
        lon_2 = df.loc[i+1, "longitude"]
        lat_1 = df.loc[i, "latitude"]
        lat_2 = df.loc[i+1, "latitude"]
        dist = gpxpy.geo.haversine_distance(lat_1, lon_1, lat_2, lon_2)
        df.set_value(i, 'Distance_Between', dist)
    return df
def slope(df):
    ###determining slope
    for i in range(len(df)-1):
        elev1 = df.loc[i,'elevation']
        elev2 = df.loc[i +1 , 'elevation']
        delta_elev = elev2-elev1
        if df.loc[i,"Distance_Between"]!= 0:
            slope = delta_elev/df.loc[i,"Distance_Between"]
            df.set_value(i, 'slope', slope)
        else:
            pass
    return df
def places_query(df):
    for i in range(len(df)):
        if df.places_type[i] ==None:
            places_all_dict = gmaps.places_nearby({u'lat': df.loc[i, 'latitude'], u'lng': df.loc[i, 'longitude']},
                                                  radius=25)  # 25 is a hyper parameter that can be learned later
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
            df.set_value(i,'num_places',num_places)
            if i%20==0 or i == len(df):
                filename = str(i) + '.pkl'
                df.to_pickle(filename)
    return df
def choice_set_coordinates(foldername):

    df = pickle_loader(foldername)

    df = places_query(df)
    return df
def df_to_sql(name,df):
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/triplabmtl')
    df.to_sql(str(name), engine)

# df_pedestrian_key = trips_pedstrian() ## getting the key
# choice_co_extractor_pickler(df_pedestrian_key, 'coordinates/choice_set_coordinates_pickles')#extract the lines and adds it to a dataframe per users and saves it as a pickle
# ##the output will be pickles, sotred in the folder name
#
# choice_set_coordinates('coordinates/choice_set_coordinates_pickles')
# df_to_sql('choice_set_coordinates',df)


# df = pickle_loader('coordinates\Choice Set\choice_set_places')
df = df.join(df.places_type.str.join('|').str.get_dummies().add_prefix('tags_'))
df['rating_average'] = df.rating.apply(np.mean,axis = 0)
df.rating_average.fillna(0, inplace = True)


elastic_weights = pd.read_csv('C:/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/elastic_net_weights.csv')
elastic_weights.drop('length', axis = 1, inplace = True)
elastic_weights.set_index('name', inplace = True)
elastic_weight_dict = elastic_weights.to_dict()
elast_dic = elastic_weight_dict['coefficient']
elas_keys_set = set(elast_dic.keys())



df['scenic_index'] = 0

for index, row in df.scene_cat.iteritems():
    coor_key = set(row.keys())
    intersect = coor_key.intersection(elas_keys_set)
    list_intersect = list(intersect)# check needed for the the key correctness
    scenic_index =[]
    if not list_intersect:
        pass
    else:
        for key in list_intersect:
            value_elastic = elast_dic[key]
            value_coor = row[key]
            value = value_elastic * value_coor
            scenic_index.append(value)
        scene_sum = np.sum(scenic_index)
        df.loc[index, 'scenic_index'] = scene_sum





