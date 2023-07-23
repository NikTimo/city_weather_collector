import datetime
import gettext
import logging
import os
import sys
import time
from http import HTTPStatus
from logging.handlers import RotatingFileHandler

import peewee
import pycountry
import requests
from dotenv import load_dotenv


load_dotenv()
RETRY_PERIOD = 3600

API_ENDPOINT_WEATHER = 'https://api.openweathermap.org/data/2.5/weather?'
API_ENDPOINT_COORD = 'http://api.openweathermap.org/geo/1.0/direct?'
API_KEY = os.getenv('API_KEY')
API_RESPONSE_LIMIT = 1
API_LANG = 'ru'
API_COOLDOWN_BETWEEN_REQUESTS = 60


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler1 = logging.StreamHandler(sys.stdout)
handler2 = RotatingFileHandler(
    'logs/collector.log', mode='w', maxBytes=5*1024*1024
)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(lineno)d - %(funcName)s - %(message)s'
)
handler1.setFormatter(formatter)
handler2.setFormatter(formatter)
logger.addHandler(handler1)
logger.addHandler(handler2)


pg_db = peewee.PostgresqlDatabase(
    os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
)


class BaseModel(peewee.Model):
    class Meta:
        database = pg_db


class City(BaseModel):
    name = peewee.CharField(unique=True, max_length=20)
    country = peewee.CharField(max_length=40)
    country_code = peewee.CharField(max_length=2)
    latitude = peewee.DecimalField(
        max_digits=6, decimal_places=3,
        auto_round=True, null=True
    )
    longitude = peewee.DecimalField(
        max_digits=6, decimal_places=3,
        auto_round=True, null=True
    )

    def __str__(self):
        return f'{self.name}, {self.country}'


class Weather(BaseModel):
    city = peewee.ForeignKeyField(
        City, on_delete='CASCADE', backref='weathers'
    )
    UTC_timestamp = peewee.TimestampField(utc=True)
    timezone = peewee.TimestampField()
    UTC_time = peewee.TimeField()
    local_time = peewee.TimeField()
    temperature = peewee.DecimalField(
        max_digits=4,
        decimal_places=2,
    )
    weather = peewee.CharField(max_length=30)
    humidity = peewee.SmallIntegerField()
    wind_speed = peewee.DecimalField(
        max_digits=5,
        decimal_places=2,
    )
    wind_deg = peewee.SmallIntegerField()

    def __str__(self):
        return f'{self.city.name}, {self.temperature}'


def read_cities_from_file():
    '''Функция чтения списка городов из текстового файла'''
    logger.debug(
        f'Запущена чтения городов из файла: '
        f'{read_cities_from_file.__name__}'
    )
    with open('city_list.txt', 'r') as file:
        return [line.strip().split(', ') for line in file]


def get_country_code_by_name_ru(country_ru):
    '''Функция получения двузначного кода страны по ISO-3166
    для названия страны на русском языке'''
    logger.debug(
        f'Запущена функция получения двузначного кода по названию страны: '
        f'{get_country_code_by_name_ru.__name__}'
    )
    gettext.translation(
        'iso3166', pycountry.LOCALES_DIR, languages=['ru']
    ).install()
    if country_ru not in [_(country.name) for country in pycountry.countries]:
        logger.error(f'Неверное название страны: {country_ru}')
    else:
        return ''.join(
            (country.alpha_2) for country in pycountry.countries
            if _(country.name) == country_ru
        )


def get_api_answer_coords(city):
    '''Функция получения координат города чере Geocoding API'''
    logger.debug(
        f'Запущена функция получения ответа API: '
        f'{get_api_answer_coords.__name__}'
    )
    payload = {
        'q': f'{city.name},{city.country_code}',
        'limit': API_RESPONSE_LIMIT,
        'appid': API_KEY
    }
    try:
        response = requests.get(API_ENDPOINT_COORD, params=payload)
        logger.debug('Получен ответ API')
    except requests.RequestException as error:
        logger.error(f'Ошибка запроса к API: {error}')
    if response.status_code != HTTPStatus.OK:
        raise requests.exceptions.HTTPError(
            f'Статус-код не 200. Статус-код: {response.status_code}'
        )
    return response.json()[0]


def get_api_answer_weather(city):
    '''Функция получения погодных условий'''
    logger.debug(
        f'Запущена функция получения ответа API: '
        f'{get_api_answer_weather.__name__}'
    )
    payload = {
        'lat': city.latitude,
        'lon': city.longitude,
        'units': 'metric',
        'lang': API_LANG,
        'appid': API_KEY
    }
    try:
        response = requests.get(API_ENDPOINT_WEATHER, params=payload)
        logger.debug('Получен ответ API')
    except requests.RequestException as error:
        logger.error(f'Ошибка запроса к API: {error}')
    if response.status_code != HTTPStatus.OK:
        raise requests.exceptions.HTTPError(
            f'Статус-код не 200. Статус-код: {response.status_code}'
        )
    return response.json()


def check_response(response, key_list):
    '''Функция првоерки наличия необходиимых ключей в ответе API'''
    logger.debug(
        f'Запущена функция проверки ответа API: '
        f'{check_response.__name__}'
    )
    if not isinstance(response, dict):
        logger.error(f'Неожиданный тип ответа: {type(response)}')
        raise TypeError(f'Неожиданный тип ответа: {type(response)}')
    for key in key_list:
        if key not in response.keys():
            logger.error(f'В ответе API отсутствуют ожидаемый ключ {key}')
            raise KeyError(f'В ответе API отсутствуют ожидаемый ключ {key}')
    logger.debug('Ответ API корректен')
    return True


def add_cities():
    '''Функция добавления новых городов в базу данных'''
    logger.debug(
        f'Запущена функция добавления городов в базу: '
        f'{add_cities.__name__}'
    )
    cities_list = read_cities_from_file()
    cities_in_db = [[city.name, city.country] for city in City.select()]
    new_cities = [
        [city, country] for [city, country] in cities_list
        if [city, country] not in cities_in_db
    ]
    cities_bulk = []
    for city in new_cities:
        object = City(
            name=city[0],
            country=city[1],
            country_code=get_country_code_by_name_ru(city[1]),
        )
        object.latitude = get_api_answer_coords(object)['lat']
        object.longitude = get_api_answer_coords(object)['lon']
        cities_bulk.append(object)

    try:
        if cities_bulk:
            City.bulk_create(cities_bulk)
            logger.info('Города успешно добавлены в базу или обновлены')
        else:
            logger.info('Новых городов не обнаружено')
    except peewee.DatabaseError as error:
        logger.error(f'Ошибка при работе с базой: {error}')


def add_weather():
    '''Функция добавления погодных условий в базу данных'''
    logger.debug(
        f'Запущена функция добавления погоды в базу: '
        f'{add_weather.__name__}'
    )
    city_list = City.select()
    weather_bulk = []
    for city in city_list:
        response = get_api_answer_weather(city)
        if check_response(
            response, ['dt', 'timezone', 'main', 'weather', 'wind']
        ):
            weather_bulk.append(Weather(
                    city=city,
                    UTC_timestamp=response['dt'],
                    timezone=response['timezone'],
                    UTC_time=datetime.datetime.utcfromtimestamp(
                        response['dt']
                    ).time(),
                    local_time=datetime.datetime.utcfromtimestamp(
                        response['dt']+response['timezone']
                    ).time(),
                    temperature=response['main']['temp'],
                    weather=response['weather'][0]['description'],
                    humidity=response['main']['humidity'],
                    wind_speed=response['wind']['speed'],
                    wind_deg=response['wind']['deg'],
            ))
    try:
        Weather.bulk_create(weather_bulk)
        logger.info('Погодные условия успешно добавлены в базу')
    except peewee.DatabaseError as error:
        logger.error(f'Ошибка при работе с базой: {error}')


def main():
    logger.info('Начало работы')
    while True:
        try:
            pg_db.connect()
            pg_db.create_tables([City, Weather])
            add_cities()
            time.sleep(API_COOLDOWN_BETWEEN_REQUESTS)
            add_weather()
        except Exception as error:
            message = f'Сбой в работе программы: {error}'
            logger.error(message, exc_info=True)
        finally:
            pg_db.close()
            time.sleep(RETRY_PERIOD)


if __name__ == '__main__':
    main()
