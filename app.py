import re
import time
import base64
import logging
from contextlib import contextmanager

import streamlit as st
from snowflake.cortex import Complete
from snowflake.snowpark.session import Session


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Initialize session states
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = {}
if 'search_results' not in st.session_state:
    st.session_state.search_results = None
if 'last_query' not in st.session_state:
    st.session_state.last_query = None
if 'llm_recommendations' not in st.session_state:
    st.session_state.llm_recommendations = None
    

# Custom styling to make it prettier
st.markdown("""
    <style>
    /* Main app styling */
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }

    /* Header styling */
    .app-header {
        text-align: center;
        padding: 3rem 0;
        margin-bottom: 2rem;
    }
    
    .app-header h1 {
        color: #FF4B6E;
        font-size: 2.8rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 0.75rem;
    }

    .app-header p {
        color: #2D3748;
        font-size: 1.1rem;
        opacity: 0.8;
    }

    .logo-container {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 1rem;
        margin-bottom: 1rem;
    }
    .app-logo {
        width: 90px;  /* Increased logo size */
        height: auto;
        animation: pulse 2s infinite;
    }
    .app-title {
        color: #FF4B6E;
        font-size: 4rem;  /* Increased font size */
        font-weight: 800;  /* Made font bolder */
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.1);  /* Subtle text shadow */
    }
    .app-subtitle {
        color: #666;
        font-size: 1.2rem;
        margin-top: 0.5rem;
    }

    
    /* Search bar styling */
    .stTextInput > div > div > input {
        border-radius: 25px;
        padding: 1rem 1.5rem;
        border: 2px solid #E2E8F0;
        font-size: 1.1rem;
    }
    .stTextInput > div > div > input:focus {
        border-color: #FF4B6E;
        box-shadow: 0 0 0 2px rgba(255, 75, 110, 0.1);
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 0 24px;
        background-color: transparent;
        border-radius: 4px 4px 0 0;
    }
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
        background-color: #FFE4E9;
    }
    .badge-container {
        display: flex;
        gap: 0.5rem;
        margin-bottom: 1rem;
        align-items: center;
    }
    .rating-badge {
        background-color: #FFB84D;
        color: white;
        padding: 0.4rem 0.8rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
    }
    .view-imdb {
        background-color: #E67E22;
        color: white !important;
        padding: 0.4rem 0.8rem;
        border-radius: 20px;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
        gap: 0.25rem;
        text-decoration: none;
    }
    .view-imdb:hover {
        opacity: 0.9;
        color: white !important;
    }

    </style>
""", unsafe_allow_html=True)


class SnowflakeConnectionManager:
    def __init__(self):
        self.last_connection_time = None
        self.connection_timeout = 3600  # 1 hour timeout
        self.session = None

    def _create_session(self):
        """Create a new Snowflake session"""
        try:
            connection_parameters = {
                "account": st.secrets['SNOWFLAKE_ACCOUNT'],
                "user": st.secrets['SNOWFLAKE_USER'],
                "password": st.secrets['SNOWFLAKE_PASSWORD'],
                "warehouse": st.secrets['SNOWFLAKE_WAREHOUSE'],
                "database": st.secrets['SNOWFLAKE_DB'],
                "schema": st.secrets['SNOWFLAKE_SCHEMA'],
                "role": st.secrets['SNOWFLAKE_ROLE']
            }
            self.session = Session.builder.configs(connection_parameters).create()
            self.last_connection_time = time.time()
            logger.info("Created new Snowflake session")
            return self.session
        except Exception as e:
            logger.error(f"Failed to create Snowflake session: {str(e)}")
            raise

    def _is_session_expired(self):
        """Check if the current session has expired"""
        if not self.last_connection_time:
            return True
        return (time.time() - self.last_connection_time) > self.connection_timeout

    def get_session(self):
        """Get a valid Snowflake session, creating a new one if necessary"""
        try:
            if not self.session or self._is_session_expired():
                if self.session:
                    try:
                        self.session.close()
                    except:
                        pass
                return self._create_session()
            
            # Test the connection with a simple query
            try:
                self.session.sql('SELECT 1').collect()
                return self.session
            except:
                logger.info("Session test failed, creating new session")
                return self._create_session()
            
        except Exception as e:
            logger.error(f"Error getting Snowflake session: {str(e)}")
            st.error("Failed to connect to Snowflake. Please try refreshing the page.")
            raise

    @contextmanager
    def session_context(self):
        """Context manager for handling Snowflake sessions"""
        session = None
        try:
            session = self.get_session()
            yield session
        except Exception as e:
            logger.error(f"Error in session context: {str(e)}")
            raise
        finally:
            # Don't close the session here, just update the last connection time
            if session:
                self.last_connection_time = time.time()

# Initialize the connection manager in session state
if 'snowflake_manager' not in st.session_state:
    st.session_state.snowflake_manager = SnowflakeConnectionManager()

@st.cache_resource(ttl=3600)  # Cache for 1 hour
def get_snowpark_session():
    """Get a Snowflake session with caching"""
    return st.session_state.snowflake_manager.get_session()

def find_similar_movies(description, session):
    """Execute movie search query with error handling"""
    try:
        with st.session_state.snowflake_manager.session_context() as session:
            return session.sql(f"""
                SELECT DISTINCT title, storyline, url, imdb_rating, genres, imdb_id, poster_url, year, top_cast, director,
                    VECTOR_COSINE_SIMILARITY(
                        storyline_voyage_embedding, 
                        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{description}')
                    ) as similarity_score
                FROM {st.secrets['SNOWFLAKE_TABLE']}
                ORDER BY similarity_score DESC
                LIMIT 50
            """).to_pandas()
    except Exception as e:
        logger.error(f"Error finding similar movies: {str(e)}")
        st.error("An error occurred while searching for movies. Please try again.")
        return None


def get_recommendations(description, similar_movies, session):
    prompt = f"""Recommend movies based on this request: "{description}"
    Similar movies found:
    {similar_movies[['TITLE', 'STORYLINE', 'IMDB_RATING', 'GENRES', 'SIMILARITY_SCORE']].to_string()}
    List movie titles from highest ranking to lowest ranking, and provide it as a python list.
    Only include this python list in your response and nothing else. Make sure the list doesn't include
    duplicates""".strip()
    
    return Complete("mistral-large2", prompt)


def perform_search(query, session):
    """Perform search with error handling"""
    if query != st.session_state.last_query:
        try:
            start_time = time.time()
            results = find_similar_movies(query, session)
            logger.info(f"Retrieved similar movies in {time.time() - start_time:.6f} seconds")

            if results is not None and not results.empty:
                start_time = time.time()
                llm_response = get_recommendations(query, results, session)
                logger.info(f"Got LLM response in {time.time() - start_time:.6f} seconds")
                
                st.session_state.search_results = results
                st.session_state.llm_recommendations = llm_response
                st.session_state.last_query = query
                return True
        except Exception as e:
            logger.error(f"Error in perform_search: {str(e)}")
            st.error("An error occurred while processing your search. Please try again.")
    return False


def update_watchlist(movie_key, movie_data, action='add'):
    """Update watchlist without triggering a rerun"""
    if action == 'add':
        st.session_state.watchlist[movie_key] = movie_data
        st.toast(f"Added {movie_data['title']} to your watchlist!")
    else:
        if movie_key in st.session_state.watchlist:
            movie_title = st.session_state.watchlist[movie_key]['title']
            del st.session_state.watchlist[movie_key]
            st.toast(f"Removed {movie_title} from your watchlist!")


def display_watchlist():
    """Display the watchlist in a separate tab"""
    if not st.session_state.watchlist:
        st.info("Your watchlist is empty. Start adding movies!")
        return

    sorted_watchlist = dict(sorted(
        st.session_state.watchlist.items(),
        key=lambda x: x[1]['added_time'],
        reverse=True
    ))

    for movie_key, movie_data in sorted_watchlist.items():
        col1, col2, col3 = st.columns([1, 2, 1])

        with col1:
            if movie_data['poster_url']:
                poster_url = movie_data['poster_url'].split('._')[0] + '.jpg'
                st.image(poster_url, width=150)
        
        with col2:
            st.markdown(f"### {movie_data['title']} ({movie_data['year']})")
            st.markdown(f"**Director:** {movie_data['director']}")
  
            st.markdown(f"""
                <div class="badge-container">
                    <a href="{movie_data['imdb_url']}" target="_blank" class="view-imdb">
                        View on IMDb
                    </a>
                </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
                <div class="badge-container">
                    <div class="rating-badge">
                        ⭐ {movie_data['rating']}/10
                    </div>
                </div>
            """, unsafe_allow_html=True)
            if st.button("Remove", key=f"watchlist_remove_{movie_key}"):
                update_watchlist(movie_key, movie_data, 'remove')
        
        st.markdown("---")


def display_search_results():
    """Display search results from session state"""
    if st.session_state.search_results is not None:
        results = st.session_state.search_results.copy()
        
        #st.write(st.session_state.llm_recommendations)
        start_time = time.time()
        
        # Extract items from the LLM response
        llm_response = st.session_state.llm_recommendations
        similar_movies = [row['TITLE'] for _, row in results.iterrows()]
        extracted_items = re.findall(r'"(.*?)"', llm_response)

        # Filter items that are in the movies list
        llm_ranked_movies = [item for item in extracted_items if item in similar_movies]
        llm_ranked_movies = list(dict.fromkeys(llm_ranked_movies))  # Remove duplicates while preserving order
        
        # Create a ranking dictionary for sorting
        rank_dict = {title: idx for idx, title in enumerate(llm_ranked_movies)}
        
        # Add a rank column to the DataFrame
        results['RANK'] = results['TITLE'].apply(lambda x: rank_dict.get(x, len(results)))
        
        # Sort the results by rank
        results = results.sort_values('RANK')
        
        for _, row in results.iterrows():
            col1, col2, col3 = st.columns([2, 2, 1])
            
            if row['DIRECTOR'] is not None:
                director = row["DIRECTOR"].split(',,')[0]
            if row['TOP_CAST'] is not None:
                top_cast = row["TOP_CAST"].split(',,')[0]
            if row['GENRES'] is not None:
                genres = row["GENRES"].split(',,')[0]
        
            if row['POSTER_URL'] is None:
                continue
            
            with col1:
                st.markdown(f"#### {row['TITLE']} ({row['YEAR']})")
                poster_url = row['POSTER_URL'].split('._')[0] + '.jpg'
                st.image(poster_url, width=200)

                st.markdown(f"""
                    <div class="badge-container">
                        <div class="rating-badge">
                            ⭐ {row['IMDB_RATING']}/10
                        </div>
                        <a href="{row['URL']}" target="_blank" class="view-imdb">
                            View on IMDb
                        </a>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.write("")
                st.markdown("**Plot**")
                plot_text = row['STORYLINE']
                if len(plot_text) > 180:
                    movie_id = f"{row['TITLE']}_{row['YEAR']}_{_}".replace(" ", "_")
                    if f"show_full_{movie_id}" not in st.session_state:
                        st.session_state[f"show_full_{movie_id}"] = False
                    
                    if st.session_state[f"show_full_{movie_id}"]:
                        st.write(plot_text)
                        if st.button("Show less", key=f"less_{movie_id}"):
                            st.session_state[f"show_full_{movie_id}"] = False
                    else:
                        st.write(f"{plot_text[:180]}...")
                        if st.button("Read more", key=f"more_{movie_id}"):
                            st.session_state[f"show_full_{movie_id}"] = True
                else:
                    st.write(plot_text)
                
                st.markdown(f"**Director:** {director}")
                st.markdown(f"**Cast:** {top_cast}")
                st.markdown(f"**Genres:** {genres}")
            
            with col3:
                st.write("")
                # Create a container for the watchlist button to enable updating
                button_container = st.empty()
                movie_key = f"{row['TITLE']}_{row['YEAR']}"
                movie_data = {
                    'title': row['TITLE'],
                    'year': row['YEAR'],
                    'rating': row['IMDB_RATING'],
                    'director': director,
                    'poster_url': row['POSTER_URL'],
                    'imdb_url': row['URL'],
                    'added_time': time.time()
                }
                
                # Use session state to track button state
                button_key = f"search_button_{movie_key}"
                if button_key not in st.session_state:
                    st.session_state[button_key] = movie_key not in st.session_state.watchlist

                if st.session_state[button_key]:
                    if button_container.button("➕ Add to Watchlist", key=f"search_add_{movie_key}"):
                        update_watchlist(movie_key, movie_data, 'add')
                        st.session_state[button_key] = False
                        button_container.button("✓ In Watchlist", key=f"search_remove_{movie_key}", disabled=True)
                else:
                    if button_container.button("✓ In Watchlist", key=f"search_remove_{movie_key}"):
                        update_watchlist(movie_key, movie_data, 'remove')
                        st.session_state[button_key] = True
                        button_container.button("➕ Add to Watchlist", key=f"search_add_{movie_key}", disabled=True)
            
            st.markdown("---")
        
        print(f"Displayed results in {time.time() - start_time:.6f} seconds")


def img_to_base64(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()


def main():
    logo_path = "assets/logo.png"
    logo_base64 = img_to_base64(logo_path)

    st.markdown(f"""
        <div class="app-header">
            <div class="logo-container">
                <img src="data:image/png;base64,{logo_base64}" class="app-logo" alt="flix.app logo"/>
                <div class="app-title">flix.app</div>
            </div>
            <div class="app-subtitle">What can we hook you up with?</div>
        </div>
    """, unsafe_allow_html=True)
    search_tab, watchlist_tab = st.tabs(["flix.app", "watchlist"])
    
    session = get_snowpark_session()
    if not session:
        return

    with search_tab:
        query = st.text_input("Search for movies:", placeholder="a dark time travel sci-fi with an anti hero")

        if query:
            # Only perform search if query is new
            perform_search(query, session)
            # Display results from session state
            display_search_results()
        else:
            # Clear search results when query is empty
            st.session_state.search_results = None
            st.session_state.last_query = None
            st.session_state.llm_recommendations = None

    with watchlist_tab:
        display_watchlist()

if __name__ == "__main__":
    main()
