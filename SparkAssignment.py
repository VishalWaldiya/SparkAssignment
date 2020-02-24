#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType
import re
import time
sc = pyspark.SparkContext(appName="test")

spark = SparkSession(sc)

moviesdf = spark.read.csv("/home/spark/Documents/spark/Dataset/ml-latest-small/movies.csv" , header=True)
ratingsdf = spark.read.csv("/home/spark/Documents/spark/Dataset/ml-latest-small/ratings.csv" , header=True)
linksdf = spark.read.csv("/home/spark/Documents/spark/Dataset/ml-latest-small/links.csv" , header=True)
tagsdf = spark.read.csv("/home/spark/Documents/spark/Dataset/ml-latest-small/tags.csv" , header=True)


# # Problem 1 

# ## How many “Drama” movies (movies with the "Drama" genre) are there? 

# In[2]:


moviesdf.show(5)


# In[3]:


moviesdf.filter(col("genres").contains('Drama')).count()


# In[4]:


moviesdf.filter(col("genres").contains('Drama')).show()


# ### Solution

# In[5]:


moviesdf.filter(col("genres").contains('Drama')).count()


# # Problem 2

# ## How many unique movies are rated, how many are not rated?

# In[6]:


ratingsdf.count()


# In[7]:


ratingsdf.where(col("rating").isNotNull()).show()


# In[70]:


ratingsdf.select("movieId").distinct().show()


# ##  SOlution

# In[9]:


ratingsdf.select("movieId").distinct().count()


# # Problem 3

# ## Who give the most ratings, how many rates did he make?

# In[10]:


ratingsdf.describe().show()


# In[11]:


ratingsdf.where(col("userId").isNotNull()).count()


# In[12]:


ratingsdf.cube("userId").count().sort(f.desc('count')).where(col('userId').isNotNull()).show()


# ## Solution

# In[13]:


user_who_rated_most = ratingsdf.cube("userId").count().sort(f.desc('count')).where(col('userId').isNotNull()).first()
print("User {} was the most aactive user with {} rating provided".format(user_who_rated_most['userId'],user_who_rated_most['count']))


# # Problem 4

# ## Compute min, average, max rating per movie.

# ## Solution

# In[14]:


ratingsdf.groupby(col('movieId')).agg(f.min('rating'),f.avg('rating'),f.max('rating')).show()


# In[ ]:





# # Problem 5

# ## Output dataset containing users that have rated a movie but not tagged it.

# In[59]:


users_who_rated_and_not_tagged = ratingsdf.join(tagsdf,['movieId'],how="left")


# In[60]:


users_who_rated_and_not_tagged.count()


# In[61]:


users_who_rated_and_not_tagged.show(5)


# In[62]:


users_who_rated_and_not_tagged.filter(col('tag')==None).show(5)


# # Problem 6

# ## Output dataset containing users that have rated AND tagged a movie.

# In[23]:



users_who_rated_and_tagged = ratingsdf.join(tagsdf,['movieId'],how="inner")

users_who_rated_and_tagged.filter(col('rating').isNull()).show()


# In[ ]:





# In[ ]:





# In[ ]:





# # Problem 7

# ## Describe how you would ﬁnd the release year for a movie (refer to the readme for information).

# In[16]:


def get_year(movieName):
    result = re.findall(r'.*([1-3][0-9]{3})',movieName)
    if result:
        return int(result[0])
    else:
        return None
    
get_year_udf = udf(lambda z: get_year(z),IntegerType())


# ##  Solution

# In[17]:


moviesdf.select('movieId','title',get_year_udf('title').alias('Year')).show()


# In[ ]:





# # Problem 8

# ## Enrich movies dataset with extract the release year. Output the enriched dataset.

# ## SOlution

# In[18]:


moviesdf_with_year = moviesdf.withColumn('Year',get_year_udf(col('title')))


# In[19]:


moviesdf_with_year.show(5)


# In[ ]:





# # Problem 9

# ##  Output dataset showing the number of movies per Genre per Year (movies will be counted many times if it's associated with multiple genres). 

# In[ ]:


moviesdf_genres_with_year.groupBy('genre','Year')


# In[ ]:


moviesdf_with_year.show(5)


# In[ ]:


moviesdf_with_year.describe().show()


# In[ ]:


moviesdf_with_year.summary().show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Problem 10

# ## Describe how you would write tests to ensure that all movies have a release year? Write these tests

# In[ ]:


movies_with_year =  moviesdf_with_year.select('year').where(col('year').isNotNull()).count()


# In[ ]:


movies_without_release_year = moviesdf_with_year.select('year').where(col('year').isNull()).count()


# In[ ]:


movies_without_release_year


# In[ ]:





# In[ ]:





# # Problem 11

# ##  Write tests to ensure that the rating & tag happened on or after the year that the movie was released (here we can only check the release year against the year of rating & tag).

# In[2]:


def epoch_to_datetime(x):
    return time.localtime(int(x)).tm_year


# In[3]:


epoch_to_datetime_udf = udf(lambda z: epoch_to_datetime(z) ,IntegerType())


# In[4]:


ratingsdf_with_year = ratingsdf.withColumn("ratings_year",epoch_to_datetime_udf(col('timestamp')))
tagsdf_with_year = tagsdf.withColumn("tags_year",epoch_to_datetime_udf(col('timestamp')))


# In[24]:


ratingsdf_with_year.join( tagsdf_with_year, ['movieId'],how='full').show(5)


# In[26]:


tagsdf.filter((col('movieId')==1090) & (col('userId')==1)).show()


# In[ ]:


ratingsdf_with_year.show(5)


# In[ ]:


tagsdf_with_year.show(5)


# In[ ]:


moviesdf_with_year.show(5)


# In[ ]:


moviesdf_genres_with_year = moviesdf_with_year.withColumn("genres", f.explode(f.split("genres","[|]")))


# In[ ]:


moviesdf_genres_with_year.show(5)


# In[ ]:





# In[ ]:


moviesdf_genres_with_year.groupBy('genres','year').count().show()


# ## With Genre

# In[ ]:


moviesandratingsdf = moviesdf_genres_with_year.join(ratingsdf_with_year , moviesdf_genres_with_year.movieId==ratingsdf_with_year.movieId).drop(ratingsdf_with_year.movieId)


# ##  Without Genre

# In[ ]:


moviesandratingsdf = moviesdf_with_year.join(ratingsdf_with_year , moviesdf_with_year.movieId==ratingsdf_with_year.movieId).drop(ratingsdf_with_year.movieId)


# In[ ]:


moviesandratingsdf.show(5)


# ###  Movies whose ratings were given after release

# In[ ]:


moviesandratingsdf.filter(col('Year')<=col('ratings_year')).show(5)


# ###  Movies whose ratings were given before release

# In[ ]:


moviesandratingsdf.filter(col('Year')>col('ratings_year')).show(5)


# In[ ]:


tagsdf_with_year.show(5)


# In[ ]:





# ## Movies Ang Tags Together

# In[20]:


moviesandtagsdf = moviesdf_genres_with_year.join(tagsdf_with_year , moviesdf_genres_with_year.movieId==tagsdf_with_year.movieId).drop(tagsdf_with_year.movieId)


# In[ ]:


moviesandtagsdf.show(5)


# ### Movies whose ratings were given after release

# In[ ]:


moviesandtagsdf.filter(col('Year')<=col('tags_year')).show(5)


# ### Movies whose ratings were given before release

# In[ ]:


moviesandtagsdf.filter(col('Year')>col('tags_year')).show(5)


# In[ ]:





# # Problem 12

# ##  Write tests to ensure that at least 50% of movies have more than one genres.

# In[ ]:


moviesdf_genres_with_year.show(5)


# In[ ]:


moviesandtagsdf.select('movieId').distinct().count()


# In[ ]:


moviesdf_genres_with_year.groupBy('movieId').count().filter(col('count') > 1).show()


# ## Movies with more than 1 Genres

# In[ ]:


moviesdf_genres_with_year.groupBy('movieId').count().filter(col('count') > 1).count()


# In[ ]:


movies_with_atleast_1_genres = moviesdf_genres_with_year.groupBy('movieId').count().filter(col('count') > 1).count()


# In[ ]:


moviesdf_genres_with_year.groupBy('movieId').count().show()


# ### All Movies with Genres Count

# In[ ]:


all_movies = moviesdf.count()


# In[ ]:


all_movies


# ###  So % of movies with atleast 1 Genres

# In[ ]:


(movies_with_atleast_1_genres/all_movies)*100


# In[ ]:





# In[ ]:





# In[ ]:


df.filter((col("rating") == 5 )).select('movieId','rating').distinct().show()


# In[ ]:


moviesdf = spark.read.csv("/home/spark/Documents/spark/Dataset/ml-25m/movies.csv" , header=True)


# In[ ]:


moviesdf.columns


# In[ ]:


moviesdf.filter((col("movieId").isin([1,2,3,4]))).show()


# In[ ]:


moviesdf.filter((col("genres").isin(['Comedy,Romance']))).show()


# In[ ]:


moviesdf.filter((col("genres").isin(['Comedy,Romance']))).show()


# In[ ]:


moviesdf.where(filter_func(['comedy','romance'],(col('geners'))))


# In[ ]:


ratingsdf.groupBy('movieId').count().show(5)


# In[ ]:


ratingsdf.groupBy('movieId').sum().show(5)


# In[ ]:


moviesdf.select('title').show(5)


# In[ ]:




