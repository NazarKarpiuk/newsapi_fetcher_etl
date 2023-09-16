import pandas as pd
import requests
from base64 import b64decode
from datetime import datetime, timedelta
from uuid import uuid4

def data_processor():
    current_date = datetime.today().date()
    api_key = ''

    base_url = "https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
    start_date_value = str(current_date - timedelta(days=1))
    end_date_value = str(current_date)

    news_df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'urlSource', 'content', 'source', 'author', 'urlToImage'])

    api_url = base_url.format('', start_date_value, end_date_value, api_key)

    response = requests.get(api_url)
    response_data = response.json()

    for article in response_data['articles']:
        news_title = article['title']
        timestamp = article['publishedAt']
        trimmed_content = "None"
        url_source = article['url']
        source = article['source']
        author = article['author']
        url_to_image = article['urlToImage']
        
        partial_content = ""
        if str(article['content']) != 'None':
            partial_content = article['content']
        
        if len(partial_content) >= 200:
            partial_content = partial_content[:199]
        
        if '.' in partial_content:
            trimmed_content = partial_content[:partial_content.rindex('.')]
        else:
            trimmed_content = partial_content
        
        news_df = pd.concat([news_df, pd.DataFrame({
            'newsTitle': news_title, 
            'timestamp': timestamp, 
            'urlSource': url_source, 
            'content': trimmed_content,
            'source': source, 
            'author': author, 
            'urlToImage': url_to_image
        })], ignore_index=True)

    file_id = str(uuid4())
    output_filename = f"~/news_{file_id}.parquet"
    deduplicated_df = news_df.drop_duplicates()
    deduplicated_df.to_parquet(output_filename)
    return output_filename