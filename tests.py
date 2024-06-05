import unittest
import sqlite3


class TestCreateTableSQL(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_create_table_success_with_primary_key(self):
        # Позитивный тест на создание таблицы c названием на английском с PRIMARY KEY
        create_table_sql = """
        CREATE TABLE cities (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80),
            location POINT
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        # Проверка существования таблицы
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'cities')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(cities);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 0, None, 0),
            (2, 'location', 'POINT', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_copy_scructure(self):
        # Позитивный тест на создание таблицы с помощью копирования структуры у другой через select
        create_table_sql = """
        CREATE TABLE cities (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80),
            location POINT
        );
        """
        self.cursor.execute(create_table_sql)

        create_table_copy_sql = """
        CREATE TABLE local_cities AS SELECT * FROM cities LIMIT 0;
        """

        self.cursor.execute(create_table_copy_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'cities')

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='local_cities';")
        copy_table = self.cursor.fetchone()
        self.assertIsNotNone(copy_table)
        self.assertEqual(copy_table[0], 'local_cities')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(cities);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 0, None, 0),
            (2, 'location', 'POINT', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_using_copy_with_data(self):
        # Позитивный тест на создание таблицы с помощью копирования части данных у другой через select
        create_table_sql = """
        CREATE TABLE cities (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80)
        );
        """
        self.cursor.execute(create_table_sql)
        self.cursor.execute("INSERT INTO cities (id, name) VALUES (1, 'Moscow'), (2, 'London'), (3, 'Milan');")

        create_table_copy_sql = """
        CREATE TABLE british_cities AS SELECT * FROM cities WHERE id = 2;
        """

        self.cursor.execute(create_table_copy_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='british_cities';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'british_cities')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(cities);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

        # Проверка существующих данных
        self.cursor.execute("SELECT * FROM british_cities;")
        user = self.cursor.fetchone()

        # Ожидаемые значения
        expected_user = (2, 'London')

        # Проверка данных
        self.assertIsNotNone(user)
        self.assertEqual(user[0], expected_user[0])  # id
        self.assertEqual(user[1], expected_user[1])  # name

    def test_create_table_success_without_primary_key(self):
        # Позитивный тест на создание таблицы c названием на английском без PRIMARY KEY
        create_table_sql = """
        CREATE TABLE cities (
            id INTEGER,
            name VARCHAR(80),
            location POINT
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'cities')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(cities);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 0),
            (1, 'name', 'VARCHAR(80)', 0, None, 0),
            (2, 'location', 'POINT', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_foreign_key_success(self):
        # Позитивный тест на создание таблицы с внешним ключом
        create_cities_sql = """
        CREATE TABLE cities (
            id INTEGER,
            name VARCHAR(80),
            location POINT
        );
        """
        create_employee_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) NOT NULL,
            age INTEGER,
            city_id INTEGER,
            FOREIGN KEY(city_id) REFERENCES cities(id)
        );
        """
        self.cursor.execute(create_cities_sql)
        self.cursor.execute(create_employee_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities';")
        city_table = self.cursor.fetchone()
        self.assertIsNotNone(city_table)
        self.assertEqual(city_table[0], 'cities')

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='employees';")
        employee_table = self.cursor.fetchone()
        self.assertIsNotNone(employee_table)
        self.assertEqual(employee_table[0], 'employees')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(employees);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 1, None, 0),
            (2, 'age', 'INTEGER', 0, None, 0),
            (3, 'city_id', 'INTEGER', 0, None, 0),
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_not_null(self):
        # Позитивный тест на создание таблицы со всеми столбцами NOT NULL
        create_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) NOT NULL,
            age INTEGER NOT NULL,
            city_id INTEGER NOT NULL,
            FOREIGN KEY(city_id) REFERENCES cities(id)
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='employees';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'employees')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(employees);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 1, None, 0),
            (2, 'age', 'INTEGER', 1, None, 0),
            (3, 'city_id', 'INTEGER', 1, None, 0),
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_unique(self):
        # Позитивный тест на создание таблицы со столбцом UNIQUE
        create_table_sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(users);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 1, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_russian_name(self):
        # Позитивный тест на создание таблицы с русским названием
        create_table_sql = """
        CREATE TABLE пользователи (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='пользователи';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

    def test_create_table_with_quotation_name(self):
        # Позитивный тест на создание таблицы в один кавычках
        create_table_sql = """
        CREATE TABLE 'пользователи' (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='пользователи';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

    def test_create_table_with_empty_name(self):
        # Позитивный тест на создание таблицы c пустым названием ''
        create_table_sql = """
        CREATE TABLE '' (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

    def test_create_table_with_symbols(self):
        # Позитивный тест на создание таблицы с числами и спец символами
        create_table_sql = """
        CREATE TABLE cities_23_$ (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities_23_$';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)


    def test_create_table_with_name_in_one_symbol(self):
        # Позитивный тест на создание таблицы с именем в один символ
        create_table_sql = """
        CREATE TABLE U (
            id INTEGER PRIMARY KEY,
            name VARCHAR(80) UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='U';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(U);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'name', 'VARCHAR(80)', 1, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_check_constraint(self):
        # Создание таблицы с `CHECK` ограничением
        create_table_sql = """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            price REAL CHECK(price > 0)
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'products')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(products);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'price', 'REAL', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_autoincrement(self):
        # Создание таблицы с автоинкрементным столбцом
        create_table_sql = """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            price REAL
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'products')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(products);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'price', 'REAL', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)


    def test_create_table_with_combine_primary_key(self):
        # Создание таблицы с комбинированным первичным ключом
        create_table_sql = """
        CREATE TABLE students (
            student_id INTEGER,
            course_id INTEGER,
            PRIMARY KEY (student_id, course_id)
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='students';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'students')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(students);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'student_id', 'INTEGER', 0, None, 1),
            (1, 'course_id', 'INTEGER', 0, None, 2)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_default_text_value(self):
        # Создание таблицы с `DEFAULT` значением для текстового столбца
        create_table_sql = """
        CREATE TABLE settings (
            id INTEGER PRIMARY KEY,
            theme TEXT DEFAULT 'light'
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'settings')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(settings);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'theme', 'TEXT', 0, "'light'", 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_default_numeric_value(self):
        # Создание таблицы с `DEFAULT` значением для числового столбца
        create_table_sql = """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            price REAL DEFAULT 0.0
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'products')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(products);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'price', 'REAL', 0, '0.0', 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_default_date_value(self):
        # Создание таблицы с `DEFAULT` значением для даты
        create_table_sql = """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            date DATE DEFAULT (DATE('now'))
        );
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)
        self.assertEqual(table[0], 'products')

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(products);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'id', 'INTEGER', 0, None, 1),
            (1, 'date', 'DATE', 0, "DATE('now')", 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_multiple_foreign_keys(self):
        # Создание таблицы с несколькими внешними ключами
        create_parent_tables_sql = """
        CREATE TABLE authors (
            author_id INTEGER PRIMARY KEY
        );
        
        CREATE TABLE publishers (
            publisher_id INTEGER PRIMARY KEY
        );
        """

        create_child_table_sql = """
        CREATE TABLE books (
            book_id INTEGER PRIMARY KEY,
            author_id INTEGER,
            publisher_id INTEGER,
            FOREIGN KEY(author_id) REFERENCES authors(author_id),
            FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
        );
        """

        for sql in create_parent_tables_sql.split(';'):
            if sql.strip():
                self.cursor.execute(sql.strip())

        self.cursor.execute(create_child_table_sql)

        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='books';")
        table = self.cursor.fetchone()
        self.assertIsNotNone(table)

        # Проверка столбцов
        self.cursor.execute("PRAGMA table_info(books);")
        columns = self.cursor.fetchall()

        # Ожидаемые столбцы с их атрибутами
        expected_columns = [
            (0, 'book_id', 'INTEGER', 0, None, 1),
            (1, 'author_id', 'INTEGER', 0, None, 0),
            (2, 'publisher_id', 'INTEGER', 0, None, 0)
        ]

        # Проверяем количество столбцов
        self.assertEqual(len(columns), len(expected_columns))

        # Проверяем атрибуты каждого столбца
        for actual_column, expected_column in zip(columns, expected_columns):
            self.assertEqual(actual_column, expected_column)

    def test_create_table_with_index(self):
        # Создание таблицы и индекса на неё
        create_table_sql = """
        CREATE TABLE indexed_table (
            id INTEGER PRIMARY KEY,
            column_to_index TEXT
        );
        
        CREATE INDEX idx_column_to_index ON indexed_table (column_to_index);
        """

        for sql in create_table_sql.split(';'):
            if sql.strip():
                self.cursor.execute(sql.strip())

        self.conn.commit()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_column_to_index';")
        index = self.cursor.fetchone()

        self.assertIsNotNone(index)

    def test_create_table_invalid_syntax(self):
        # Негативный тест на создание таблицы с неверным синтаксисом (нет запятой после PRIMARY KEY)
        create_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_invalid_integer_name(self):
        # Негативный тест на создание таблицы с числовым названием
        create_table_sql = """
        CREATE TABLE 23 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_invalid_date_name(self):
        # Негативный тест на создание таблицы с названием в виде даты
        create_table_sql = """
        CREATE TABLE 2.07.2023 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_invalid_symbols_name(self):
        # Негативный тест на создание таблицы с недопустимыми спец символами в названии таблицы
        create_table_sql = """
        CREATE TABLE est!@#$%^&*()  (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_invalid_symbols_name(self):
        # Негативный тест на создание таблицы с двумя словами в названии таблицы
        create_table_sql = """
        CREATE TABLE my customer  (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_with_dublicate_name(self):
        # Негативный тест на создание таблицы с дублирующим названием
        create_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        self.cursor.execute(create_table_sql)
        create_duble_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            department TEXT
        );
        """

        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_duble_table_sql)

    def test_create_table_duplicate_column_name(self):
        # Негативный тест на создание таблицы с дублирующимся именем столбца
        create_table_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        );
        """
        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_without_column(self):
        # Негативный тест на создание таблицы без полей
        create_table_sql = """
        CREATE TABLE cities ();
        """

        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_insert_null_into_not_null_column(self):
        # Негативный тест на вставку null в колонку с ограничением not null
        create_table_sql = """
        CREATE TABLE test_not_null (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)

        with self.assertRaises(sqlite3.IntegrityError):
            self.cursor.execute("INSERT INTO test_not_null (id, name) VALUES (1, NULL);")

    def test_insert_duplicate_into_unique_column(self):
        # Негативный тест на дубликат в уникальном поле UNIQUE
        create_table_sql = """
        CREATE TABLE test_unique (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)

        self.cursor.execute("INSERT INTO test_unique (id, name) VALUES (1, 'user1');")

        with self.assertRaises(sqlite3.IntegrityError):
            self.cursor.execute("INSERT INTO test_unique (id, name) VALUES (2, 'user1');")

    def test_insert_duplicate_into_primary_key_column(self):
        # Негативный тест на дубликат в PRIMARY KEY
        create_table_sql = """
        CREATE TABLE test_unique (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """
        self.cursor.execute(create_table_sql)

        self.cursor.execute("INSERT INTO test_unique (id, name) VALUES (1, 'user1');")

        with self.assertRaises(sqlite3.IntegrityError):
            self.cursor.execute("INSERT INTO test_unique (id, name) VALUES (1, 'user2');")

    def test_create_table_with_duplicate_primary_key(self):
        # Негативный тест на создание таблицы с дублирующимся первичным ключом
        create_table_sql = """
        CREATE TABLE duplicate_primary_key (
            id INTEGER PRIMARY KEY,
            another_id INTEGER PRIMARY KEY
        );
        """

        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_table_with_invalid_check_constraint(self):
        # Негативный тест на создание таблицы с некорректным ограничением `CHECK`
        create_table_sql = """
       CREATE TABLE invalid_check_constraint (
           id INTEGER PRIMARY KEY,
           value REAL CHECK(value > )
       );
       """

        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_table_sql)

    def test_create_index_with_invalid_syntax(self):
        # Негативный тест на создание таблицы с некорректным синтаксисом создания индекса
        create_table_sql = """
       CREATE TABLE valid_indexed_table (
           id INTEGER PRIMARY KEY,
           column_to_index TEXT
       );"""
        create_index = """
       CREATE IND invalid_syntax_index ON valid_indexed_table (column_to_index);
       """
        self.cursor.execute(create_table_sql)

        with self.assertRaises(sqlite3.OperationalError):
            self.cursor.execute(create_index)


if __name__ == '__main__':
    unittest.main()
