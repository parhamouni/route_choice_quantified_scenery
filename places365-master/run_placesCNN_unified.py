# PlacesCNN to predict the scene category, attribute, and class activation map in a single pass
# by Bolei Zhou, sep 2, 2017

import torch
from torch.autograd import Variable as V
import torchvision.models as models
from torchvision import transforms as trn
from torch.nn import functional as F
import os, glob
import numpy as np
from scipy.misc import imresize as imresize
# import cv2
from PIL import Image
from functools import partial
import pickle
import urllib
import urllib.request
import io
import time
# my files mainly
import psycopg2
import datetime
import pandas as pd
import googlemaps
import sqlalchemy
import re
import gc

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]

def connect(user, password, db, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta
## connecting to postgres for saving the tables in there
con= connect('postgres','postgres','triplabmtl')

## connecting to googlemaps api with the key

gmaps = googlemaps.Client(key='AIzaSyAwN7PG2RVVBUXv2TixB_BoAxlUWjTZErM')

try:
        conn = psycopg2.connect("dbname='triplabmtl' user='postgres' host='localhost' password= 'postgres' ")
except:
        print ("I am unable to connect to the database")
cur = conn.cursor()


###  Bolei Zhou, sep 2, 2017, Parham Hamouni have partially changed it!


def load_labels():
    # prepare all the labels
    # scene category relevant
    file_name_category = 'categories_places365.txt'
    if not os.access(file_name_category, os.W_OK):
        synset_url = 'https://raw.githubusercontent.com/csailvision/places365/master/categories_places365.txt'
        os.system('wget ' + synset_url)
    classes = list()
    with open(file_name_category) as class_file:
        for line in class_file:
            classes.append(line.strip().split(' ')[0][3:])
    classes = tuple(classes)

    # indoor and outdoor relevant
    file_name_IO = 'IO_places365.txt'
    if not os.access(file_name_IO, os.W_OK):
        synset_url = 'https://raw.githubusercontent.com/csailvision/places365/master/IO_places365.txt'
        os.system('wget ' + synset_url)
    with open(file_name_IO) as f:
        lines = f.readlines()
        labels_IO = []
        for line in lines:
            items = line.rstrip().split()
            labels_IO.append(int(items[-1]) -1) # 0 is indoor, 1 is outdoor
    labels_IO = np.array(labels_IO)

    # scene attribute relevant
    file_name_attribute = 'labels_sunattribute.txt'
    if not os.access(file_name_attribute, os.W_OK):
        synset_url = 'https://raw.githubusercontent.com/csailvision/places365/master/labels_sunattribute.txt'
        os.system('wget ' + synset_url)
    with open(file_name_attribute) as f:
        lines = f.readlines()
        labels_attribute = [item.rstrip() for item in lines]
    file_name_W = 'W_sceneattribute_wideresnet18.npy'
    if not os.access(file_name_W, os.W_OK):
        synset_url = 'http://places2.csail.mit.edu/models_places365/W_sceneattribute_wideresnet18.npy'
        os.system('wget ' + synset_url)
    W_attribute = np.load(file_name_W)

    return classes, labels_IO, labels_attribute, W_attribute

def hook_feature(module, input, output):
    features_blobs.append(np.squeeze(output.data.cpu().numpy()))

def returnCAM(feature_conv, weight_softmax, class_idx):
    # generate the class activation maps upsample to 256x256
    size_upsample = (256, 256)
    nc, h, w = feature_conv.shape
    output_cam = []
    for idx in class_idx:
        cam = weight_softmax[class_idx].dot(feature_conv.reshape((nc, h*w)))
        cam = cam.reshape(h, w)
        cam = cam - np.min(cam)
        cam_img = cam / np.max(cam)
        cam_img = np.uint8(255 * cam_img)
        output_cam.append(imresize(cam_img, size_upsample))
    return output_cam

def returnTF():
# load the image transformer
    tf = trn.Compose([
        trn.Scale((224,224)),
        trn.ToTensor(),
        trn.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    return tf

def load_model():
    # this model has a last conv feature map as 14x14

    model_file = 'whole_wideresnet18_places365.pth.tar'
    if not os.access(model_file, os.W_OK):
        os.system('wget http://places2.csail.mit.edu/models_places365/' + model_file)
        os.system('wget https://raw.githubusercontent.com/csailvision/places365/master/wideresnet.py')
    useGPU = 0
    pickle.load = partial(pickle.load, encoding="latin1")
    pickle.Unpickler = partial(pickle.Unpickler, encoding="latin1")
    model = torch.load(model_file, map_location=lambda storage, loc: storage, pickle_module=pickle)
    # if useGPU == 1:
    #     model = torch.load(model_file)
    # else:
    #     model = torch.load(model_file, map_location=lambda storage, loc: storage) # allow cpu

    ## if you encounter the UnicodeDecodeError when use python3 to load the model, add the following line will fix it. Thanks to @soravux




    model.eval()
    # hook the feature extractor
    features_names = ['layer4','avgpool'] # this is the last conv layer of the resnet
    for name in features_names:
        model._modules.get(name).register_forward_hook(hook_feature)
    return model

# load the labels
classes, labels_IO, labels_attribute, W_attribute = load_labels()

# load the model
features_blobs = []
model = load_model()

# load the transformer
tf = returnTF() # image transformer

# get the softmax weight
params = list(model.parameters())
weight_softmax = params[-2].data.numpy()
weight_softmax[weight_softmax<0] = 0

def scene(img_url):
    """it takes img_url and provides you with scene categries and scene attributes of all of the heading, which are 35"""

    # load the test image

    # img_url = 'https://maps.googleapis.com/maps/api/streetview?size=1200x1200&location=45.4970487,-73.5812098&fov=90&heading=120&pitch=0&key=AIzaSyDtl22p4sitje7XQWYlGKwb7gaxhF8bhiE'
    with urllib.request.urlopen(img_url) as url:
        f = io.BytesIO(url.read())
    img = Image.open(f)

    input_img = V(tf(img).unsqueeze(0), volatile=True) # HERE IS WHERE THE INPUT IS PASSED TO THE MODEL, I HAVE TO CHANGE THIS IN HERE

    # forward pass
    logit = model.forward(input_img)
    h_x = F.softmax(logit).data.squeeze()
    probs, idx = h_x.sort(0, True)
    # print 'RESULT ON ' + img_url
    # output the IO prediction
    io_image = np.mean(labels_IO[idx[:10].numpy()]) # vote for the indoor or outdoor
    # if io_image < 0.5:
        # print ('--TYPE OF ENVIRONMENT: indoor')
    # else:
        # print ('--TYPE OF ENVIRONMENT: outdoor')

    # output the prediction of scene category
    # scene categories, in a dictionary
    # print ('--SCENE CATEGORIES:')
    scene_cat ={}
    for i in range(0, 5):
        if classes[idx[i]] in scene_cat:
            scene_cat[classes[idx[i]]].append([probs[i]])
        else:
            scene_cat[classes[idx[i]]]= [probs[i]]
        # scene_cat[classes[idx[i]]] = probs[i]
        # print('{:.3f} -> {}'.format(probs[i], classes[idx[i]]))
    # print (scene_cat)

    # output the scene attributes
    responses_attribute = W_attribute.dot(features_blobs[1])
    idx_a = np.argsort(responses_attribute)
    # print ('--SCENE ATTRIBUTES:')
    # print (', '.join([labels_attribute[idx_a[i]] for i in range(-1,-10,-1)]))
    scene_att = [labels_attribute[idx_a[i]] for i in range(-1,-10,-1)]
    # print(scene_att)
    return scene_cat, scene_att

def load_image(lat,lng):

    # img_pd= pd.DataFrame(columns = ['lat','lng', 'heading','query_url'])
    api_key = 'AIzaSyDtl22p4sitje7XQWYlGKwb7gaxhF8bhiE'
    heading = [x for x in range(0, 360, 90)]    # P: THE HEADING WILL GIVE ME A 360 DEGREE OVERVIEW OF THE COORDINATE
    pitch = 0 # p : IT IS SUITABLE FOR THE USAGE WHICH IS HUMAN EYE VISION
    # Size didn't matter in querying, i made the biggest one!
    img_scene = {}
    img_att = []
    for h in heading:
        img_query ='https://maps.googleapis.com/maps/api/streetview?size=1200x1200&location={},{}&fov=90&heading={}&pitch={}&key={}'.format(lat,lng, h, pitch,api_key )
        img_scene_temp,img_att_temp = scene(img_query)
        img_temp_keys = img_scene_temp.keys()
        img_scene_keys = img_scene.keys()
        for key in img_temp_keys:
            if key in img_scene_keys:
                img_scene[key].extend(img_scene_temp[key])
            else:
                img_scene[key] = img_scene_temp[key]
        ## right now I have created the scenaries for each of the point in detail, I add them up based on the assumption that if they have been detected more times, they have been
        ## better explanotary of the environment
        # print(img_att_temp)
        img_att.extend(img_att_temp)

        ### summing up:

    for key in img_scene.keys():
        list = img_scene[key]
        summy = 0

        for i in list:
            summy = summy + i
        img_scene[key]= summy


        # img_temp = pd.Series([lat,lng,h,img_query,img_scene,img_att],index = ['lat','lng', 'heading','query_url','scene_cat','scene_att'])
        # img_pd = img_pd.append(img_temp,ignore_index=True)
    return img_scene, img_att
###  in the top was the mixture of Bolei Zho and me

def scene_query(df):
    """Assumes df is a dataframe for a single user_trip, returns the  scenes categories as a list for each row of coordinates"""

    for i in range(len(df)):
        try:
            if df.loc[i,'scene_cat'] is None:
                scene_cat, scene_att = load_image(df.loc[i,'lat'],  df.loc[i, 'lng'])
                df.at[i, 'scene_cat']= scene_cat
                df.at[i, 'scene_att']= scene_att
                if i % 5 ==0:
                    time.sleep(0.5)
                if i%100 == 0:
                    pickle_name = 'coordinates_revealed_with_scene_{}.pkl'.format(str(i))
                    df.to_pickle(pickle_name)
                print(i/len(df)*100, datetime.datetime.now())
                gc.collect()

        except urllib.error.HTTPError:
            pass
    return df

def pickle_loader():
    # df_final = pd.DataFrame()
    pickle_names = pickle_finder()
    for index, pickler in enumerate(pickle_names):
        pickler = "/mnt/c/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC" + pickler
        with (open(pickler, "rb")) as openfile:
            while True:
                try:
                    df = pickle.load(openfile)
                    # df_final = pd.concat([df_final,df], ignore_index=True)
                except EOFError:
                    break
    return df

def pickle_finder():
    # os.chdir("/mnt/c/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/") + foldername + "/"
    pickle_names = []
    for file in glob.glob("*.pkl"):
        pickle_names.append(file)
    return pickle_names

try:
        conn = psycopg2.connect("dbname='triplabmtl' user='postgres' host='localhost' password= 'postgres' ")
except:
        print("I am unable to connect to the database")
cur = conn.cursor()


#
#
# sql_temp = "SELECT * FROM coordinates_choice_set_final"
# cur.execute(sql_temp)
# col_names = []
# for elt in cur.description:
#     col_names.append(elt[0])
#
# df = pd.DataFrame(cur.fetchall(), columns=col_names)
# df.set_index('index')

# df_final = pd.DataFrame()
os.chdir(
    "/mnt/c/Users/parha/Google Drive/My projects/Thesis/Inputs/Walking_image_processing/THC/Data Prep/Revealed")
# pickle_names = pickle_finder()
# pickle_names.sort(key=natural_keys)
# pickle_name = pickle_names[-1]

# pickler = os.getcwd() + '/' + pickle_name
# print(pickler)
# with (open(pickler, "rb")) as openfile:
#     while True:
#         try:
#             df = pickle.load(openfile)
#             # df_final = pd.concat([df_final,df], ignore_index=True)
#         except EOFError:
#             break

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/triplabmtl')
# df = pd.read_sql_table('F_coordinates_revealed',engine)
# df['scene_cat'] = None
# df['scene_att'] = None
df = pd.read_pickle('coordinates_revealed_with_scene_6200.pkl')
df = scene_query(df)
df.to_pickle('coordinates_choice_scenery_final_3.pkl')




# for index, id_user_trip in df_keys.iterrows():
#     pickle_name = 'coordinates_choice_set_final_with_scene.pkl'
#
#     pickle_names = pickle_finder()
#     if pickle_name not in pickle_names:
#
#
#         sql_temp = """
#                        SELECT * FROM coordinates_pedestrian_30 where trip_index = {} ;
#                        """.format(index)
#         cur.execute(sql_temp)
#         conn.commit()
#
#         ct = cur.fetchall()
#
#         col_names = []
#         for elt in cur.description:
#             col_names.append(elt[0])
#
#         df = pd.DataFrame(ct, index=list(range(len(ct))), columns=col_names)
#
#
#         try:
#
#         except:
#             print(index)

# #
# # # generate class activation mapping
# # print ('Class activation map is saved as cam.jpg')
# # # CAMs = returnCAM(features_blobs[0], weight_softmax, [idx[0]])
#
#
# # render the CAM and output
# # img = cv2.imread('test.jpg')
# # height, width, _ = img.shape
# # heatmap = cv2.applyColorMap(cv2.resize(CAMs[0],(width, height)), cv2.COLORMAP_JET)
# # result = heatmap * 0.4 + img * 0.5
# cv2.imwrite('cam.jpg', result)

