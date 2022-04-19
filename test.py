import requests
from bs4 import BeautifulSoup as bs


def get_session_id(raw_resp):
    soup = bs(raw_resp.text, 'lxml')
    token = soup.find_all('input', {'name':'survey_session_id'})[0]['value']
    return token

# payload = {
#     'f213054909': 'o213118718',  # 21st checkbox
#     'f213054910': 'Ronald',  # first input-field
#     'f213054911': 'ronaldG54@gmail.com',
#     }

payload = {
    'username': 'bitcom-90@mail.ru',
    'password': 'Nurislam177@'
}

headers = {'accept': '*/*',
           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) snap Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}

url = r'https://kaspi.kz/merchantcabinet/login#/offers'

with requests.session() as s:
    print('asdasd')
    # resp = s.get(url, timeout=5, headers=headers)
    resp = s.post(url, headers=headers, data=payload)
    soup = bs(resp.content, 'html.parser')
    print(soup)
    # payload['survey_session_id'] = get_session_id(resp)
    # response_post = s.post(url, data=payload)
    # print response_post.text
