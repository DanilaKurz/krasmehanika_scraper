import itertools
import pandas as pd
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import httplib2
import apiclient.discovery


scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('krasmehaika-scrapper-074958bc7c2c.json', scope)
client = gspread.authorize(creds)
spreadsheet_id = '1q8QRvSQL7TsBuK5MzBa1yW2gAZiFwxAH5vthbWa7Yzg'

sheet = client.open("product").sheet1

products_df = pd.DataFrame(
    columns=['Ссылка', 'Категория', 'Подкатегория', 'Название', 'Описание', 'Характеристики', 'Цена', 'Измерение',
             'Количество', 'Артикул', 'KP link',
             'Youtube_link', 'Actuality'])

base_url = 'https://krasmehanika.ru/catalog'


def convert_table_to_text(data):
    df = pd.DataFrame(data)
    text = df.to_string(index=False)

    return text


def table_to_text(soupecky):
    # Преобразуем HTML-таблицу в объект BeautifulSoup

    # Находим все строки таблицы
    rows = soupecky.find_all('tr')

    # Извлекаем заголовки таблицы
    headers = [header.text.strip() for header in rows[0].find_all('td')]

    # Извлекаем данные таблицы
    data = []
    for row in rows[1:]:
        values = [value.text.strip() for value in row.find_all('td')]
        data.append(values)

    # Создаем датафрейм
    df = pd.DataFrame(data, columns=headers)

    # Преобразуем датафрейм в текст
    text = df.to_string(index=False)

    return text


# Использование функции
data = {
    'Диаметр сопла (дюймы)': [0.013, 0.015, 0.017, 0.019, 0.021, 0.023, 0.025, 0.027, 0.029, 0.031, 0.033, 0.035, 0.039,
                              0.043, 0.055],
    'Производительность вода 138 бар (л/мин)': [0.69, 0.91, 1.17, 1.47, 1.79, 2.15, 2.54, 2.96, 3.42, 3.90, 4.42, 4.98,
                                                6.18, 7.51, 12.29],
    'Ширина факела 5 см': [213, 215, 217, 219, None, None, None, None, None, None, None, None, None, None, None],
    'Ширина факела 10 см': [None, 315, 317, 319, 321, 323, 325, 327, 329, 331, 333, 335, 339, 343, 355],
    'Ширина факела 15 см': [None, None, 417, 419, 421, 423, 425, 427, 429, 431, None, 435, 439, 443, None],
    'Ширина факела 30 см': [None, None, None, None, 621, 623, 625, 627, 629, 631, None, 635, 639, 643, None]
}


def extract_and_format_text(soupca_bi):
    contents = []
    if soupca_bi:
        for element in soupca_bi:
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                contents.append(f"**{element.get_text(strip=True)}**")
            elif element.name == 'p':
                contents.append(f"{element.get_text(strip=True)}")
            elif element.name == 'ul':
                for li in element.find_all('li'):
                    contents.append(f"*{li.get_text(strip=True)}")
            elif element == soupca_bi.find('table', {'border': '1', 'cellspacing': '0', 'cellpadding': '0'}):
                contents.append(convert_table_to_text(data))
            elif element == soupca_bi.find('table', {'cellpadding': '0'}):
                contents.append(table_to_text(soupca_bi))
            elif isinstance(element, str):
                contents.append(element.strip())
            elif element.name == 'br':
                contents.append("")
        clean_text = '\n'.join(line for line, _ in itertools.groupby(contents))
    else:
        clean_text = 'No info'
    return clean_text


# функция для извлечения характеристик
def extract_characteristics(soupchik):
    table = soupchik.find('table', class_='props_list')

    characteristics = []
    if table:
        for row in table.find_all('tr', itemprop='additionalProperty'):
            char_name = row.find('td', class_='char_name').find('span', itemprop='name').get_text(strip=True)
            char_value = row.find('td', class_='char_value').find('span', itemprop='value').get_text(strip=True)
            characteristics.append(f"{char_name}: {char_value}")

        characteristics_strings = "\n".join(characteristics)
    else:
        characteristics_strings = 'No info'

    return characteristics_strings


def extract_price(soupcheg):
    price_element = soupcheg.find('div', class_='price', attrs={'data-value': True})

    if price_element:
        price_value = price_element['data-value']

        if int(price_value) != 0:
            price_string = int(price_value)
        else:
            price_string = 'No info'

    else:
        price_string = 'No info'

    return price_string


def extract_currency(soupcheg):
    price_element = soupcheg.find('div', class_='price', attrs={'data-value': True})

    if price_element:
        price_value = price_element['data-value']
        currency_element = price_element.find('span', class_='price_currency')
        measure_element = price_element.find('span', class_='price_measure')

        currency = currency_element.get_text(strip=True) if currency_element else ""
        measure = measure_element.get_text(strip=True) if measure_element else ""

        if int(price_value) != 0:
            price_string = f"{currency} {measure}".strip()
        else:
            price_string = 'No info'
    else:
        price_string = 'No info'

    return price_string


def extract_availability(soupecky):
    availability_element = soupecky.find('div', class_='item-stock')

    if availability_element:
        value_element = availability_element.find('span', class_='value')
        if value_element:
            value_text = value_element.get_text(strip=True)
    else:
        value_text = 'No info'

    return value_text


def extract_youtube_link(tochka):
    youtube_link_element = tochka.find('div', class_='video_block')

    # Пытаемся найти iframe в элементе video_block
    iframe_element = youtube_link_element.find('iframe') if youtube_link_element else None

    if iframe_element:
        # Извлекаем ссылку из атрибута src
        youtube_link_str = iframe_element['src']
    elif youtube_link_element:
        # Если iframe не найден, но элемент video_block существует, используем его текст
        youtube_link_str = youtube_link_element.text.strip()
    else:
        youtube_link_str = 'No info'

    return youtube_link_str


def extract_article_number(another_soup):
    article_element = another_soup.find('span', class_='block_title')
    article_value = None

    if article_element:
        value_element = article_element.find_next_sibling('span', class_='value')
        if value_element:
            article_value = value_element.get_text(strip=True)

    if article_value:
        atricle_value_str = article_value
    else:
        atricle_value_str = 'No info'

    return atricle_value_str


def extract_download_link(vkusno):
    link_element = vkusno.find('a', string='Скачать КП')

    if link_element and link_element.has_attr('href'):
        return f'https://krasmehanika.ru{link_element["href"]}'

    return 'No info'


#
def extract_category_and_subcategory(link, sopec):
    # Parse the URL
    parsed_url = urlparse(link)
    segments = parsed_url.path.strip('/').split('/')

    # Initialize default values
    category_text = 'No info'
    subcategory_text = 'No info'

    # Check the number of segments to determine the type of URL
    if len(segments) in [3, 4, 5, 6]:
        # Construct the category URL and extract its name
        category_url = urljoin(link, f"/{segments[0]}/{segments[1]}/")
        category_response = requests.get(category_url)
        category_soup = BeautifulSoup(category_response.text, 'lxml')
        category_text = category_soup.find('h1', id='pagetitle').get_text(strip=True)

        # If there are more than 3 segments, construct the subcategory URL and extract its name
        if len(segments) > 3:
            subcategory_url = urljoin(link, f"/{segments[0]}/{segments[1]}/{segments[2]}/")
            subcategory_response = requests.get(subcategory_url)
            subcategory_soup = BeautifulSoup(subcategory_response.text, 'lxml')
            subcategory_text = subcategory_soup.find('h1', id='pagetitle').get_text(strip=True)

    return category_text, subcategory_text


unique_links = []

counter = 0


with open('links.txt', 'r') as file:
    links = [line.strip() for line in file.readlines()]

for item in links:
    product_response = requests.get(item)
    product_soup = BeautifulSoup(product_response.text, 'lxml')

    product_name = product_soup.find('h1', id='pagetitle').text
    product_description = extract_and_format_text(product_soup.find('div', class_='detail_text'))
    product_characteristics = extract_characteristics(product_soup)
    product_price = extract_price(product_soup)
    product_availability = extract_availability(product_soup)
    product_article = extract_article_number(product_soup)
    kp_link = extract_download_link(product_soup)
    youtube_link = extract_youtube_link(product_soup)
    category, subcategory = extract_category_and_subcategory(item, product_soup)
    measure = extract_currency(product_soup)

    # if existing_product is not None:
    # 	actuality = existing_product['Actuality']
    # else:
    actuality = 0  # or set it based on your new parsing

    products_df.loc[len(products_df)] = [item, category, subcategory, product_name, product_description,
                                         product_characteristics,
                                         product_price, measure, product_availability, product_article, kp_link,
                                         youtube_link,
                                         actuality]

    row_data = [item, category, subcategory, product_name, product_description,
                product_characteristics, product_price, measure, product_availability,
                product_article, kp_link, youtube_link, actuality]

    sheet.append_row(row_data)


    counter += 1
    print(counter)
    time.sleep(2)

# products_df.to_excel('products.xlsx', index=False)
