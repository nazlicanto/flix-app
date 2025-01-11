## flix.app

A minimalistic movie recommender built with Snowflake and Streamlit. flix.app performs RAG for real-time movie recommendation using `voyage-3-multilingual` text embeddings and [Mistral Large 2](https://mistral.ai/technology/#models), and features in-app watchlist.

![flix.app Main Page](https://github.com/nazlicanto/flix-app/blob/main/assets/main.jpeg "flix.app")


### Usage
Create your own dataset using the [BrightData/IMDb-Media](https://huggingface.co/datasets/BrightData/IMDb-Media) Hugging Face dataset and the [Cinemagoer](https://cinemagoer.readthedocs.io/) library. To create a csv file with all data, except text embeddings:
```sh
pip install -r requirements.txt
python create_database.py
```

You can then either upload the csv file to Snowflake to create a database and table, and create text embeddings with an SQL query:
```sql
CREATE OR REPLACE TABLE YOUR_DATABASE.YOUR_SCHEMA.YOUR_TABLE_NAME AS
SELECT
    *,
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', storyline) AS storyline_voyage_embedding
FROM YOUR_DATABASE.YOUR_SCHEMA.YOUR_TABLE_NAME;
```

To run the Streamlit app, you need to replace the placeholder Snowflake credential values in `secrets.toml` with your own credentials and copy it to `~/.streamlit/secrets.toml` for MacOS/Linux or `%userprofile%/.streamlit/secrets.toml` for Windows. Then, run the app with:
```
streamlit run app.py
```


