# City_weather_collector
## Коллектор для сбора погодных данных с API Open Weather и сохраненния их в БД
Данный коллектор позволяет загружать погодные данные для списка городов из файла city_list.txt и сохранять их в базе данных.
По умолчанию в файле составлен список из 50 крупнейших городов мира. При редактировании списка необходимо указывать точные названия стран и городов.
## Стек технолгий:
- база данных: PostgreSQL
- ORM: Peewee,
- PgAdmin
## Запуск проекта:
Создайте файл .env на основе файла .env.example.
Проект подготовлен к запуску в docker-контейнерах и начнет работу после запуска.
~~~bash
docker compose up --build
~~~
После заупска отобразятся сообщения логгера.
Для запуска в режиме демона используйте:
~~~bash
docker compose down && docker compose up --build -d
~~~
## Автор: NT
