from biogeme import *
from headers import *
from loglikelihood import *
from statistics import *

# ASC_1 = Beta('ASC_1',0,-1000,1000,0)
# ASC_3 = Beta('ASC_3',0,-1000,1000,0)

B_DISTANCE = Beta('B_DISTANCE',0,-1000,1000,0)
B_AVG_SCENIC_INDEX = Beta('B_AVG_SCENIC_INDEX',0,-1000,1000,0)
B_AVG_SLOPE =  Beta('B_AVG_SLOPE',0,-1000,1000,0)
B_MIN_SLOPE =  Beta('B_MIN_SLOPE',0,-1000,1000,0)
B_MAX_SLOPE = Beta('B_MAX_SLOPE',0,-1000,1000,0)
B_VAR_SLOPE = Beta('B_VAR_SLOPE',0,-1000,1000,0)
B_VAR_SCENIC_INDEX = Beta('B_VAR_SCENIC_INDEX',0,-1000,1000,0)
B_NUM_POINTS = Beta('B_NUM_POINTS',0,-1000,1000,0)
B_NUMBER_OF_TURNS = Beta('B_NUMBER_OF_TURNS',0,-1000,1000,0)
B_AVG_RATING_AVG = Beta('B_AVG_RATING_AVG',0,-1000,1000,0)
B_POINT_OF_INTEREST = Beta('B_POINT_OF_INTEREST',0,-1000,1000,0)
B_FOOD_STORE = Beta('B_FOOD_STORE',0,-1000,1000,0)
B_TRANSIT_STATION = Beta('B_TRANSIT_STATION',0,-1000,1000,0)
B_BUS_STATION = Beta('B_BUS_STATION',0,-1000,1000,0)
B_PS = Beta('B_PS',0,-1000,1000,0)





# Utility functions
V1 = B_DISTANCE * distance_1  +B_AVG_SCENIC_INDEX * avg_scenic_index_1\
+ B_NUM_POINTS * numpoints_1 + B_NUMBER_OF_TURNS * number_of_turns_1 +B_FOOD_STORE *sum_tags_food_1 + B_PS * ps2_1
V2 = B_DISTANCE * distance_2  + B_NUM_POINTS * numpoints_2 +B_AVG_SCENIC_INDEX * avg_scenic_index_2\
+ B_NUMBER_OF_TURNS * number_of_turns_2 + B_FOOD_STORE *sum_tags_food_2  + B_PS * ps2_2
V3 =  B_DISTANCE * distance_3  + B_AVG_SCENIC_INDEX * avg_scenic_index_3\
+ B_NUM_POINTS * numpoints_3 + B_NUMBER_OF_TURNS * number_of_turns_3 +  B_FOOD_STORE *sum_tags_food_3 + B_PS * ps2_3
#
V = {1: V1,
     2: V2,
     3: V3}

av = {1: 1,
      2: 2,
      3: 3}
logprob = bioLogLogit(V,av,choice)
rowIterator('obsIter')
BIOGEME_OBJECT.ESTIMATE = Sum(logprob,'obsIter')
# exclude = ( purpose_other == 1 ) 
# BIOGEME_OBJECT.EXCLUDE = exclude
choiceSet = [1,2,3]
cteLoglikelihood(choiceSet,choice,'obsIter')
availabilityStatistics(av,'obsIter')

