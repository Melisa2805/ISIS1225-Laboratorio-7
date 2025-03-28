"""
 * Copyright 2020, Departamento de sistemas y Computación,
 * Universidad de Los Andes
 *
 * Desarrollado para el curso ISIS1225 - Estructuras de Datos y Algoritmos
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along withthis program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contribuciones
 *
 * Dario Correal
 """

import os
import csv
import time
import tracemalloc

# TODO Realice la importación del mapa linear probing
from DataStructures.Map import map_linear_probing as lp
# TODO Realice la importación de ArrayList como estructura de datos auxiliar para sus requerimientos
from DataStructures.List import array_list as al
# TODO Realice la importación del mapa separate chaining
from DataStructures.Map import map_separate_chaining as mp

data_dir = os.path.dirname(os.path.realpath('__file__')) + '/Data/GoodReads/'

def new_logic():
    """
    Inicializa el catálogo de libros. Crea una lista vacía para guardar
    los libros y utiliza tablas de hash para almacenar los datos restantes con diferentes índices
    utilizando linear probing como tipo de tabla de hash
    """
    catalog = {"books": None,
               "books_by_id": None,
               "books_by_year_author": None,
               "books_by_authors": None,
               "tags": None,
               "book_tags": None}

    # Lista que contiene la totalidad de los libros cargados
    catalog['books'] = al.new_list()

    # Tabla de Hash que contiene los libros indexados por goodreads_book_id  
    # (good_reads_book_id -> book)
    catalog['books_by_id'] = lp.new_map(1000, 0.7)  # TODO completar la creación del mapa

    # Tabla de Hash con la siguiente pareja llave valor: (author_name -> List(books))
    catalog['books_by_authors'] = lp.new_map(1000, 0.7)  # TODO completar la creación del mapa

    # Tabla de Hash con la siguiente pareja llave valor: (tag_name -> tag)
    catalog['tags'] = lp.new_map(1000, 0.7)  # TODO completar la creación del mapa

    # Tabla de Hash principal con sub-mapas: (author_name -> (original_publication_year -> list(books)))
    catalog['books_by_year_author'] = lp.new_map(1000, 0.7)  # TODO completar la creación del mapa
    
    # Tabla de Hash con la siguiente pareja llave valor: (tag_id -> book_tags)
    catalog['book_tags'] = lp.new_map(1000, 0.7)

    return catalog

#  -------------------------------------------------------------
# Funciones para la carga de datos
#  -------------------------------------------------------------

def load_data(catalog):
    """
    Carga los datos de los archivos y carga los datos en la
    estructura de datos
    """
    books, authors = load_books(catalog)
    tag_size = load_tags(catalog)
    book_tag_size = load_books_tags(catalog)
    return books, authors, tag_size, book_tag_size

def load_books(catalog):
    """
    Carga los libros del archivo. Por cada libro se toman sus autores y por
    cada uno de ellos, se crea en la lista de autores, a dicho autor y una
    referencia al libro que se está procesando.
    """
    booksfile = data_dir + "books.csv"
    input_file = csv.DictReader(open(booksfile, encoding='utf-8'))
    for book in input_file:
        add_book(catalog, book)
    return book_size(catalog), author_size(catalog)

def load_tags(catalog):
    """
    Carga todos los tags del archivo y los agrega a la lista de tags
    """
    tagsfile = data_dir + 'tags.csv'
    input_file = csv.DictReader(open(tagsfile, encoding='utf-8'))
    for tag in input_file:
        add_tag(catalog, tag)
    return tag_size(catalog)

def load_books_tags(catalog):
    """
    Carga la información que asocia tags con libros.
    """
    bookstagsfile = data_dir + "book_tags.csv"
    input_file = csv.DictReader(open(bookstagsfile, encoding='utf-8'))
    for booktag in input_file:
        add_book_tag(catalog, booktag)
    return book_tag_size(catalog)

def new_tag(name, id):
    """
    Estructura que almacena los tags utilizados para marcar libros.
    """
    tag = {"name": "", "tag_id": ""}
    tag['name'] = name
    tag['tag_id'] = id
    return tag

def new_book_tag(tag_id, book_id, count):
    """
    Estructura que crea una relación entre un tag y
    los libros que han sido marcados con dicho tag.
    """
    book_tag = {'tag_id': tag_id, 'book_id': book_id, 'count': count}
    return book_tag

#  -----------------------------------------------
# Funciones para agregar información al catálogo
#  -----------------------------------------------

def add_book(catalog, book):
    """
    Adiciona un libro al mapa de libros.
    Además, guarda la información de los autores en las tablas de hash correspondientes.
    """
    # Se adiciona el libro a la lista general de libros
    al.add_last(catalog['books'], book)
    # Se adiciona el libro a la tabla de hash indexada por goodreads_book_id
    lp.put(catalog['books_by_id'], book['goodreads_book_id'], book)
    # Se obtienen los autores del libro
    authors = book['authors'].split(",")
    # Para cada autor, se agrega en la tabla de hash indexada por autores y 
    # en las tablas de hash indexadas por autor y por año de publicación
    for author in authors:
        author = author.strip()
        add_book_author(catalog, author, book)
        add_book_author_and_year(catalog, author, book)
    return catalog

def add_book_author(catalog, author_name, book):
    """
    Adiciona un autor al mapa de autores, la cual guarda referencias
    a los libros de dicho autor.
    """
    authors = catalog['books_by_authors']
    author_value = lp.get(authors, author_name)
    if author_value:
        # Si el autor ya se había agregado, se obtiene la lista de sus libros y se agrega el nuevo libro.
        al.add_last(author_value, book)
    else:
        # Si es un autor nuevo, se crea una lista y se agrega al mapa.
        authors_books = al.new_list()
        al.add_last(authors_books, book)
        lp.put(authors, author_name, authors_books)
    return catalog

def add_book_author_and_year(catalog, author_name, book):
    """
    Adiciona un autor a los mapas indexados por autor y por año de publicación.
    Si el autor ya se había agregado: 
        - Si el año de publicación también se había agregado, se obtiene la lista y se agrega el libro.
        - Si el año de publicación no se había agregado, se crea una nueva lista para ese año y se agrega el libro.
    Si el autor no se había agregado:
        - Se crea el índice del nuevo autor, se crea un mapa para los años y se agrega una lista con el libro.
    """
    books_by_year_author = catalog['books_by_year_author']
    pub_year = book['original_publication_year']
    #TODO Completar manejo de los escenarios donde el año de publicación es vacío.
    if pub_year == '' or pub_year is None:
        pub_year = "Unknown"
    
    author_map = lp.get(books_by_year_author, author_name)
    if author_map:
        year_list = lp.get(author_map, pub_year)
        if year_list:
            al.add_last(year_list, book)
        else:
            new_list = al.new_list()
            al.add_last(new_list, book)
            lp.put(author_map, pub_year, new_list)
    else:
        # TODO Completar escenario donde no se había agregado el autor al mapa principal
        new_year_map = lp.new_map(1000, 0.7)
        new_list = al.new_list()
        al.add_last(new_list, book)
        lp.put(new_year_map, pub_year, new_list)
        lp.put(books_by_year_author, author_name, new_year_map)
    return catalog

def add_tag(catalog, tag):
    """
    Adiciona un tag al mapa de tags indexado por nombre del tag.
    """
    t = new_tag(tag['tag_name'], tag['tag_id'])
    lp.put(catalog['tags'], tag['tag_name'], t)
    return catalog

def add_book_tag(catalog, book_tag):
    """
    Adiciona un tag a la lista de book_tags.
    Si el book_tag ya había sido agregado:
        - Se obtiene la lista asociada y se agrega el nuevo book_tag.
    Si el book_tag no había sido agregado:
        - Se crea el índice en el mapa y se asocia una nueva lista con el book_tag.
    """
    t = new_book_tag(book_tag['tag_id'], book_tag['goodreads_book_id'], book_tag['count'])
    if lp.contains(catalog['book_tags'], t['tag_id']):
        book_tag_list = lp.get(catalog['book_tags'], t['tag_id'])
        al.add_last(book_tag_list, book_tag)
    else:
        new_list = al.new_list()
        al.add_last(new_list, book_tag)
        lp.put(catalog['book_tags'], t['tag_id'], new_list)  # TODO Completar escenario donde el book_tag no se había agregado al mapa
    return catalog

#  -------------------------------------------------------------
# Funciones de consulta
#  -------------------------------------------------------------

def get_book_info_by_book_id(catalog, good_reads_book_id):
    """
    Retorna toda la información almacenada de un libro según su goodreads_book_id.
    """
    # TODO Completar función de consulta
    return lp.get(catalog['books_by_id'], good_reads_book_id)

def get_books_by_author(catalog, author_name):
    """
    Retorna la lista de libros asociados al autor ingresado.
    """
    # TODO Completar función de consulta
    books = lp.get(catalog['books_by_authors'], author_name)
    if books is None:
        # Si no se encuentra el autor, se retorna el nombre y una lista vacía.
        return author_name, al.new_list()
    return author_name, books

def get_books_by_tag(catalog, tag_name):
    """
    Retorna la lista de libros que fueron etiquetados con el tag_name especificado.
      - Se obtiene el tag asociado al tag_name.
      - Se obtiene el tag_id a partir del tag.
      - Con el tag_id se obtiene la lista de book_tags y se relaciona con los datos completos del libro.
    """
    # TODO Completar función de consulta
    tag = lp.get(catalog['tags'], tag_name)
    if tag is None:
        return None
    tag_id = tag['tag_id']
    book_tags_list = lp.get(catalog['book_tags'], tag_id)
    if book_tags_list is None:
        return None
    books = al.new_list()
    size_bt = al.size(book_tags_list)
    for i in range(size_bt):
        bt = al.get_element(book_tags_list, i)
        if 'book_id' in bt:
            book_id = bt['book_id']
        else:
            book_id = bt['goodreads_book_id']
        book_info = lp.get(catalog['books_by_id'], book_id)
        if book_info is not None:
            al.add_last(books, book_info)
    return books

def get_books_by_author_pub_year(catalog, author_name, pub_year):
    """
    Retorna la lista de libros asociados a un autor y un año de publicación específicos.
    También retorna mediciones de tiempo y memoria utilizadas en la consulta.
    """
    # Si el año es vacío se reemplaza por un valor simbólico.
    if pub_year == '' or pub_year is None:
        pub_year = "Unknown"
    
    # Iniciar medición de tiempo
    start_time = getTime()
    # Iniciar medición de memoria
    tracemalloc.start()
    start_memory = getMemory()
    
    author_map = lp.get(catalog['books_by_year_author'], author_name)
    if author_map is None:
        result = al.new_list()  # Autor no encontrado; se retorna lista vacía.
    else:
        result = lp.get(author_map, pub_year)
        if result is None:
            result = al.new_list()  # No se encontraron libros para el año indicado.
    
    # Detener medición de memoria
    stop_memory = getMemory()
    # Calcular tiempo y memoria transcurrida
    end_time = getTime()
    tiempo_transcurrido = deltaTime(end_time, start_time)
    memoria_usada = deltaMemory(start_memory, stop_memory)
    
    return result, tiempo_transcurrido, memoria_usada

#  -------------------------------------------------------------
# Funciones para obtener el tamaño de los mapas
#  -------------------------------------------------------------

def book_size(catalog):
    return lp.size(catalog['books_by_id'])

def author_size(catalog):
    return lp.size(catalog['books_by_authors'])

def tag_size(catalog):
    return lp.size(catalog['tags'])

def book_tag_size(catalog):
    return lp.size(catalog['book_tags'])

#  -------------------------------------------------------------
# Funciones para obtener memoria y tiempo
#  -------------------------------------------------------------

def getTime():
    """
    Devuelve el instante de tiempo de procesamiento en milisegundos.
    """
    return float(time.perf_counter() * 1000)

def getMemory():
    """
    Toma una muestra de la memoria alocada en un instante de tiempo.
    """
    return tracemalloc.take_snapshot()

def deltaTime(end, start):
    """
    Devuelve la diferencia entre tiempos de procesamiento muestreados.
    """
    return float(end - start)

def deltaMemory(start_memory, stop_memory):
    """
    Calcula la diferencia en memoria alocada entre dos instantes y
    devuelve el resultado en kBytes.
    """
    memory_diff = stop_memory.compare_to(start_memory, "filename")
    delta_memory = sum(stat.size_diff for stat in memory_diff) / 1024.0
    return delta_memory
