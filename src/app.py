# PREVIAMENTE se llevo a cabo los pasos del archivo 'INSTRUCTIONS.es.md'



# 0.1) Importo las librerias necesarias

import os                                       # Para utilizar funciones de mi sistema operativo
import pandas as pd
from sqlalchemy import create_engine, text      # Para conectarme a mi base de datos:
                                                    # crete_engige me permite crear una conexion (motor) a mi base de datos
                                                    # text me permite escribir en lenguaje SQL como texto        
from dotenv import load_dotenv                  # Para usar mi archivo .env





# 0.2) Load environment variables

load_dotenv()               # Uso la funcion para cargar mi archivo .env con mi informacion sensible
                            # Por defecto: 1) me carga .env 2) En la raiz del proyecto





# 1) Connect to the database with SQLAlchemy

# Uso la funcion suministrada en el archivo 'INSTRUCTIONS.es.md':
# Carga cada variable de entorno (las variables de mi .env) en una variable llamada connection_string
# Luego se usa las funciones create_engine y connect


def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")

        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()

        print("Connected successfully!")
        return engine
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()

if engine is None:
    exit() 





# 2) Create the tables

# Creo las tablas indicadas en el archivo 'create.sql'
# Usamos engine.connect()
# Aca haremos uso de text de sqlalchemy
# Debido a error presentado despues de ejecutado por 1ra vez el script, se anade IF NO EXIST a cada tabla


with engine.connect() as connection:
    connection.execute(text("""
                            
    CREATE TABLE IF NOT EXISTS publishers(
        publisher_id INT NOT NULL,
        name VARCHAR(255) NOT NULL,
        PRIMARY KEY(publisher_id)
    );

    CREATE TABLE IF NOT EXISTS authors(
        author_id INT NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        middle_name VARCHAR(50) NULL,
        last_name VARCHAR(100) NULL,
        PRIMARY KEY(author_id)
    );

    CREATE TABLE IF NOT EXISTS books(
        book_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        total_pages INT NULL,
        rating DECIMAL(4, 2) NULL,
        isbn VARCHAR(13) NULL,
        published_date DATE,
        publisher_id INT NULL,
        PRIMARY KEY(book_id),
        CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id)
    );

    CREATE TABLE IF NOT EXISTS book_authors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY(book_id, author_id),
        CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE,
        CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE
    );
    """))





# 3) Insert data

# Inserto los registros para cada tabla, indicados en el archivo 'insert.sql'
# Usamos nuevamente engine.connect()
# Aca nuevamente haremos uso de text de sqlalchemy
# Debido a error presentado despues de ejecutado por 1ra vez el script, se anade ON CONFLICT DO NOTHING a cada registro


with engine.connect() as connection:
    connection.execute(text("""
                            
    INSERT INTO publishers(publisher_id, name) VALUES (1, 'O Reilly Media') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (2, 'A Book Apart') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (3, 'A K PETERS') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (4, 'Academic Press') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (5, 'Addison Wesley') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (6, 'Albert&Sweigart') ON CONFLICT DO NOTHING;
    INSERT INTO publishers(publisher_id, name) VALUES (7, 'Alfred A. Knopf') ON CONFLICT DO NOTHING;
    
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (1, 'Merritt', null, 'Eric') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (2, 'Linda', null, 'Mui') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (3, 'Alecos', null, 'Papadatos') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (4, 'Anthony', null, 'Molinaro') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (5, 'David', null, 'Cronin') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (6, 'Richard', null, 'Blum') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (7, 'Yuval', 'Noah', 'Harari') ON CONFLICT DO NOTHING;
    INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES (8, 'Paul', null, 'Albitz') ON CONFLICT DO NOTHING;

    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (1, 'Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (2, 'Facing the Intelligence Explosion', 91, 3.87, null, '2013-02-01', 7) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (3, 'Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (4, 'Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (5, 'Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (6, 'Computing machinery and intelligence', 24, 4.17, null, '2009-03-22', 4) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (7, 'XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (8, 'SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (9, 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6) ON CONFLICT DO NOTHING;
    INSERT INTO books (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES (10, 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7) ON CONFLICT DO NOTHING;

    INSERT INTO book_authors (book_id, author_id) VALUES (1, 1) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (2, 8) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (3, 7) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (4, 6) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (5, 5) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (6, 4) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (7, 3) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (8, 2) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (9, 4) ON CONFLICT DO NOTHING;
    INSERT INTO book_authors (book_id, author_id) VALUES (10, 1) ON CONFLICT DO NOTHING;
    """))




# 4) Use Pandas to read and display a table

# Imprimo (leo) las 4 tablas creadas con sus datos insertados

df1 = pd.read_sql("SELECT * FROM publishers;", engine)
print(df1)

df2 = pd.read_sql("SELECT * FROM authors;", engine)
print(df2)

df3 = pd.read_sql("SELECT * FROM books;", engine)
print(df3)

df4 = pd.read_sql("SELECT * FROM book_authors;", engine)
print(df4)


# Se ejecuta el actual script de python desde la consola respectiva e imprime las tablas correctamente
# En la consola de SQL se verifica mediante \d que a la base de datos le fueron anadidas posee dichas tablas