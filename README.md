### Как запустить проект
Скрипт работает с СУБД mysql. Для начала работы нужно:
1. Создать виртуальное окружение 
```
python3.10 -m venv venv
```
2. Активировать виртуальное окружение
```
source venv/bin/activate  (venv\Scripts\activate для Windows)
```
3. Установить зависимости 
```
pip install -r requirements.txt
```
4. Установить подключение к MySQL: 

БД MySQL можно запустить через docker

```
  docker run --name='my_sql_container' -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=mysql mysql/mysql-server
```

Если не получается подключиться к БД, то зайти внутрь контейнера с БД
  ```
    docker exec -it my_sql_container bash
  ```

  Открыть клиент mysql c паролем 'mysql'
  ```
    mysql -u root -p
  ```

  Создать пользователя dbeaver c паролем dbeaver
  ```
    CREATE USER 'dbeaver'@'%' IDENTIFIED BY 'dbeaver';
    GRANT ALL PRIVILEGES ON *.* TO 'dbeaver'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;
  ```

  Подключиться к БД localhost:3306
  Пользователь: dbeaver
  Пароль: dbeaver

5. Создать 2 БД с названиями prod и dev
- Создать разные таблицы, изменять их и попробовать запускать файл main.py
  Пример:
    ```
        CREATE DATABASE prod;
        CREATE DATABASE dev;
        CREATE TABLE dev.user(id INTEGER PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100), email VARCHAR(100) UNIQUE);      
    ```

- Запустить main.py, в результате в папке проекта будут создаваться файлы миграций вида 'migration_2023-23-59-59-59'
  Также в терминале будут описаны предлагаемые изменения с запросом на применение миграций.

- Миграции можно применить сразу нажав y/д или отменить для более подробного изучения, нажав любую другую кнопку 
- В некоторых ситуациях (добавление индексов/foreign key) миграции будут рассчитываться и применяться ступенчато

