import os
import json
import requests
from dotenv import load_dotenv
import re
import nltk
import polars as pl
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from app.logger import logger



load_dotenv()
api_key = os.environ.get('YOUTUBE_API_KEY')


def get_comments(response: requests.models.Response) -> list:
    """
        Function to extract YouTube comments-thread data from GET request response
    """
    logger.debug(f'Extracting comments from data')

    comment_thread_list = []
    
    for raw_item in json.loads(response.text)['items']:
        comment_thread_list.append(raw_item['snippet']['topLevelComment']['snippet']['textDisplay'])

    return comment_thread_list


# define channel ID
# videoId = '7SWvDHvWXok'


def extract_pipeline(videoId:str) -> list:
    logger.debug(f'Extracting data for video_id: {videoId}')

    # Google Data API Url
    url = 'https://youtube.googleapis.com/youtube/v3/commentThreads'

    # initialize page token
    page_token = None

    # intialize list to store video data
    comment_thread_list = []


    # extract video data across multiple search result pages
    while page_token != 0:
        # define parameters for API call
        params = {"key": api_key,'part': ["snippet","replies"], 'videoId': videoId, 'maxResults':100, 'pageToken': page_token}
        logger.debug(f'params : {params}')
        # make get request
        response = requests.get(url, params=params)
        logger.debug(f'Response code: {response.status_code} , {response.text}')
        # append video records to list
        comment_thread_list += get_comments(response)

        try:
            # grab next page token
            page_token = json.loads(response.text)['nextPageToken']
        except:
            # if no next page token kill while loop
            page_token = 0

    return comment_thread_list

#################################################
#TRANSFORM


# Download required NLTK resources
nltk.download('stopwords')
nltk.download('wordnet')

# Initialize stopwords and lemmatizer
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

# Function to clean and preprocess text
def preprocess_text(text):
    text = re.sub(r'http\S+|www\S+|https\S+', '', text)  # Remove URLs
    text = re.sub(r'[^a-zA-Z\s]', '', text)  # Remove special chars, numbers, emojis
    text = text.lower()  # Convert to lowercase
    words = text.split()  # Tokenize
    return ' '.join(words)

# ETL Pipeline Function
def transform_pipeline(comments)-> pl.DataFrame:
    # Step 2: Transform - Clean and preprocess the comments
    logger.debug(f'Transforming data.')

    preprocessed_comments = [preprocess_text(comment) for comment in comments]
    
    # Step 3: Load - Store in DataFrame and return
    df_comments = pl.DataFrame({
        'original_comment': comments,
        'cleaned_comment': preprocessed_comments
    })
    
    return df_comments
##################################################
#LOAD
def load_pipeline(df_comments:pl.DataFrame):
    # Save or further process the data
    logger.debug(f'Saving data in csv and parquet file.')

    df_comments.write_csv('app/data/cleaned_comments.csv')
    df_comments.write_parquet('app/data/comments.parquet')
    # Display the preprocessed DataFrame


####################################################################
#ETL PIPELINE
def etl_pipeline(videoID:str):
    comment_thread_list = extract_pipeline(videoId=videoID)
    cleaned_comments = transform_pipeline(comment_thread_list)
    load_pipeline(cleaned_comments)
