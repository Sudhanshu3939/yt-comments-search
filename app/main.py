from fastapi import FastAPI
from app.model import query_db
from app.logger import logger
app = FastAPI()


# API operations
@app.get("/")
def health_check():
    logger.debug(f"health_check API is called.")
    return {'health_check': 'OK'}

@app.get("/info")
def info():
    logger.debug(f"info API is called.")
    return {'name': 'yt-comments-search', 'description': "Search API for YouTube videos comments."}

@app.get("/getComments")
def get_comments(video_id:str, query:str, n_results:str):
    logger.debug(f"getComments API is called with params {video_id}, {query}, {n_results}")
    result = query_db(video_id=video_id, query=query, n_results=n_results)
    return {'results': result}
