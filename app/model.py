import chromadb
from sentence_transformers import SentenceTransformer
import polars as pl
from pathlib import Path
from app.data_pipeline import *
from app.logger import logger


def create_embbedings(video_id:str):

    
    # Step 1: Load Sentence-Transformers model
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

    # client = chromadb.Client()
    client = chromadb.PersistentClient(path="./app/chroma_db")

    if video_id in [c.name for c in client.list_collections()]:
        logger.debug(f'Data already exists for video_id: {video_id}')
        return client.get_collection(name=video_id), model

    logger.debug(f'Creating embeddings for video_id: {video_id}')

    # Step 2: Load your preprocessed comments
    etl_pipeline(videoID=video_id)
    df_comments = pl.read_csv('app/data/cleaned_comments.csv')

    # Extract the cleaned comments column
    # cleaned_comments = df_comments['cleaned_comment'].tolist()
    cleaned_comments = pl.Series(df_comments.select('cleaned_comment')).to_list()

    # Step 3: Generate embeddings for each comment
    embeddings = model.encode(cleaned_comments)

    # Step 5: Create a collection (like a table) for your embeddings
    # collection = client.create_collection("comment_collection")
    collection = client.get_or_create_collection(name=video_id) # Get a collection object from an existing collection, by name. If it doesn't exist, create it.

    # Step 6: Insert embeddings and metadata into the collection
    for i, (comment, embedding) in enumerate(zip(cleaned_comments, embeddings)):
        collection.add(
            ids=[str(i)],  # A unique ID for each entry
            embeddings=[embedding],  # Embedding vector
            metadatas=[{'comment': comment}]  # Optional metadata (original comment)
        )

    return collection,model


def query_db(video_id:str, query:str, n_results:str):

    # Step 7: Query the database with a new query embedding
    # query = "atom theory is stupid"  # Your search query
    
    collection,model = create_embbedings(video_id=video_id)
    query_embedding = model.encode([query])[0]  # Get embedding for the query

    # Perform the search in the ChromaDB collection
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5  # Get top 5 similar comments
    )

    return results
