from os import environ
from typing import Optional
import pandas
import requests
from dotenv import load_dotenv

load_dotenv()

def read_data():
    dataframe_a = pandas.read_csv('File A.csv')
    dataframe_b = pandas.read_csv('File B.csv')

    return pandas.merge(dataframe_a, dataframe_b, on='user_id')

def get_user_id(email: str) -> Optional[str]:
    params = {
        "aid": environ.get('AID'),
        "email": email,
        "api_token": environ.get('API_KEY')
    }

    response = requests.get(f"{environ.get('API_URL')}/publisher/user/search", params=params)
    users = response.json().get('users', [])
    if len(users) == 0:
        return None
    return users[0].get('uid')

data = read_data()

for index, row in data.iterrows():
    user_id = get_user_id(email=row['email'])
    if user_id:
        data.at[index, 'user_id'] = user_id

data.to_csv('Merged Users.csv', index=False, encoding='utf-8-sig')
