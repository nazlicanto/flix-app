import re
import ast
import time
import pandas as pd
from imdb import Cinemagoer
from datasets import load_dataset
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial


def get_imdb_instance():
    """Create a new IMDb instance for each process"""
    return Cinemagoer()

def extract_imdb_id(url):
    """Extract IMDb ID from URL"""
    try:
        return url.split('/')[-2][2:]  # Remove 'tt' prefix
    except:
        return None

def fetch_movie_details(imdb_id, ia=None):
    """Fetch both year and director from IMDb API"""
    if ia is None:
        ia = get_imdb_instance()
    try:
        movie_data = ia.get_movie(imdb_id)
        return {
            'plot': movie_data.get('plot outline', movie_data.get('plot', [None])[0] if movie_data.get('plot') else None),
            'synopsis': str(movie_data.get('synopsis')[0]) if movie_data.get('synopsis') else None,
            'year': str(movie_data.get('year')) if movie_data.get('year') else None,
            'poster_url': movie_data.get('cover url', None),
            'director': [d['name'] for d in movie_data.get('directors', [])][:1][0] if movie_data.get('directors') else None
        }
    except Exception as e:
        print(f"Error fetching details for movie {imdb_id}: {str(e)}")
        return {'plot': None, 'synopsis': None, 'year': None, 'poster_url': None, 'director': None}

def extract_year(date_str):
    """Extract the year from a date string using regex"""
    if pd.isna(date_str) or not date_str:
        return None
    match = re.search(r'\b(\d{4})\b', date_str)
    return str(match.group(1)) if match else None

def extract_genre(genre_str):
    """Extract genres from the genre string"""
    if pd.isna(genre_str) or not genre_str:
        return None
    
    try:
        genre_list = ast.literal_eval(genre_str)
        return ", ".join(genre_list)
    except:
        return None

def extract_director(credit_list):
    """Extract the director's name from the credit list"""
    if pd.isna(credit_list) or not credit_list:
        return None 
    
    try:
        credit_list = ast.literal_eval(credit_list)
        directors = []

        for credit in credit_list:
            if credit['title'] in ['Director', 'Directors']:
                directors.extend(name_dict['name'] for name_dict in credit['names'])
        
        return ", ".join(directors) if directors else None 
    except Exception as e:
        print(f"Error extracting director: {str(e)}")
        return None

def extract_cast(cast_list):
    """Extract the top cast names from the top_cast list"""
    if pd.isna(cast_list) or not cast_list:
        return None 

    try:
        cast_list = ast.literal_eval(cast_list)
        all_cast = [cast['actor'] for cast in cast_list]
        all_cast = ", ".join(all_cast[:5])
        return all_cast
    except Exception as e:
        print(f"Error extracting cast: {str(e)}")
        return None

def process_chunk(df_chunk):
    """Process a chunk of the DataFrame"""
    ia = get_imdb_instance()
    results = []
    
    for _, row in df_chunk.iterrows():
        movie_details = fetch_movie_details(row['imdb_id'], ia)
        
        # Update the row with new information
        row['plot'] = movie_details['plot']
        row['poster_url'] = movie_details['poster_url']
        
        # Update year and director if missing
        if pd.isna(row['year']) and movie_details['year']:
            row['year'] = movie_details['year']
        
        if pd.isna(row['director']) and movie_details['director']:
            row['director'] = movie_details['director']
            
        results.append(row)
    
    return pd.DataFrame(results)

def load_and_process_movie_data(chunk_size=10):
    # Load the dataset
    dataset = load_dataset("BrightData/IMDb-Media")
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(dataset['train'])
    
    # Select required columns
    df = df[[
        'title', 'storyline', 'url', 'imdb_rating', 'details_release_date',
        'poster_url', 'genres', 'credit', 'top_cast'
    ]]
    
    # Remove rows where title, storyline or poster_url are empty/NA
    df = df.dropna(subset=['title', 'storyline', 'poster_url'])
    
    # Convert rating to float and filter for ratings above 6.5
    df['imdb_rating'] = pd.to_numeric(df['imdb_rating'], errors='coerce')
    df = df[df['imdb_rating'] > 6.5]

    # Process existing data
    df['genres'] = df['genres'].apply(extract_genre)
    df['imdb_id'] = df['url'].apply(extract_imdb_id)
    df['year'] = df['details_release_date'].apply(extract_year)
    df['director'] = df['credit'].apply(extract_director)
    df['top_cast'] = df['top_cast'].apply(extract_cast)
    
    # Drop unnecessary columns
    df = df.drop(columns=['credit', 'details_release_date'])
    
    # Reset index after filtering
    df = df.reset_index(drop=True)
    
    # For testing, limit to first n entries
    #df = df[:1000]  # Uncomment and modify for testing

    # Initialize plot column
    df['plot'] = None
    
    # Split DataFrame into chunks
    df_chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    # Set up multiprocessing pool
    num_processes = min(cpu_count(), len(df_chunks))  # Don't create more processes than chunks
    print(f"Using {num_processes} processes")
    
    # Process chunks in parallel with progress bar
    with Pool(processes=num_processes) as pool:
        # Use tqdm to show progress
        results = list(tqdm(
            pool.imap(process_chunk, df_chunks),
            total=len(df_chunks),
            desc="Processing movies"
        ))
    
    # Combine results
    final_df = pd.concat(results, ignore_index=True)
    
    # Print statistics about missing data
    print("\nFinal Statistics:")
    print(f"Missing years: {final_df['year'].isna().sum()}")
    print(f"Missing directors: {final_df['director'].isna().sum()}")
    
    return final_df

if __name__ == "__main__":
    # Set chunk size based on your needs
    CHUNK_SIZE = 20
    
    start = time.time()
    movies_df = load_and_process_movie_data(chunk_size=CHUNK_SIZE)
    print(f"\nTotal movies after filtering: {len(movies_df)}")
    print("\nFirst few entries:")
    print(movies_df[['title', 'year', 'director']].head())
    end = time.time()
    print(f"Processed dataset in {end-start} seconds.")
    # Save to CSV
    movies_df.to_csv("movies.csv", index=False)