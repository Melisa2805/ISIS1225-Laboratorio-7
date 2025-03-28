from DataStructures.List.list_node import new_single_node
from DataStructures.List.list_node import get_element as get_node_info

def new_list():
    """
    Crea una lista de tipo single_linked_list vacía.
    Returns:
        dict: Un diccionario con tres llaves:
              - 'size': 0
              - 'first': None
              - 'last': None
    """
    return {
        'size': 0,
        'first': None,
        'last': None,
        'type': 'single_linked_list'
    }

def is_empty(my_list):
    """
    Verifica si la lista está vacía.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
    Returns:
        bool: True si la lista está vacía, False en caso contrario.
    """
    return my_list['size'] == 0

def size(my_list):
    """
    Retorna el tamaño de la lista.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
    Returns:
        int: Tamaño de la lista (número de elementos).
    """
    return my_list['size']

def add_first(my_list, element):
    """
    Agrega un nuevo nodo al inicio de la lista y aumenta el tamaño de la lista en 1.
    En caso de que la lista esté vacía, el primer y último nodo de la lista serán el nuevo nodo.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
        element (any): Elemento a agregar al inicio de la lista.
    Returns:
        dict: La lista (single_linked_list) con el elemento agregado al inicio.
    """
    new_node = new_single_node(element)
    if my_list['size'] == 0:
        my_list['first'] = new_node
        my_list['last'] = new_node
    else:
        new_node['next'] = my_list['first']
        my_list['first'] = new_node
    my_list['size'] += 1
    return my_list

def add_last(my_list, element):
    """
    Agrega un nuevo nodo al final de la lista y aumenta el tamaño de la lista en 1.
    En caso de que la lista esté vacía, el primer y último nodo de la lista serán el nuevo nodo.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
        element (any): Elemento a agregar al final de la lista.
    Returns:
        dict: La lista (single_linked_list) con el elemento agregado al final.
    """
    new_node = new_single_node(element)
    if my_list['size'] == 0:
        my_list['first'] = new_node
        my_list['last'] = new_node
    else:
        my_list['last']['next'] = new_node
        my_list['last'] = new_node
    my_list['size'] += 1
    return my_list

def first_element(my_list):
    """
    Retorna el primer elemento de una lista no vacía.
    Si la lista está vacía, lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
    Returns:
        any: El valor (info) del primer nodo de la lista.
    Raises:
        IndexError: Si la lista está vacía.
    """
    if my_list['size'] == 0:
        raise IndexError("list index out of range")
    return get_node_info(my_list['first'])

def last_element(my_list):
    """
    Retorna el último elemento de una lista no vacía.
    Si la lista está vacía, lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 0,
                            'first': None,
                            'last': None
                        }
    Returns:
        any: El valor (info) del último nodo de la lista.
    Raises:
        IndexError: Si la lista está vacía.
    """
    if my_list['size'] == 0:
        raise IndexError("list index out of range")
    return get_node_info(my_list['last'])

def get_element(my_list, pos):
    """
    Retorna el elemento en la posición dada.
    La posición 'pos' debe ser >= 0 y < my_list['size'].
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
        pos (int): Posición del elemento a retornar.
    Returns:
        any: El valor (info) del nodo en la posición 'pos'.
    Raises:
        IndexError: Si la posición es inválida (pos < 0 o pos >= my_list['size']).
    """
    if pos < 0 or pos >= my_list['size']:
        raise IndexError("list index out of range")
    current = my_list['first']
    for _ in range(pos):
        current = current['next']    
    return get_node_info(current)

def delete_element(my_list, pos):
    """
    Elimina el elemento en la posición 'pos' de la lista (single_linked_list).
    La posición 'pos' debe ser >= 0 y < my_list['size'].
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
        pos (int): Posición del elemento a eliminar.
    Returns:
        dict: La lista (single_linked_list) actualizada.
    Raises:
        IndexError: Si la posición es inválida (pos < 0 o pos >= my_list['size']).
    """
    if pos < 0 or pos >= my_list['size']:
        raise IndexError("list index out of range")
    if pos == 0:
        first_node = my_list['first']
        my_list['first'] = first_node['next']
        if my_list['size'] == 1:
            my_list['last'] = None
    else:
        previous = my_list['first']
        for _ in range(pos - 1):
            previous = previous['next']
        node_to_delete = previous['next']
        previous['next'] = node_to_delete['next']
        if node_to_delete == my_list['last']:
            my_list['last'] = previous
    my_list['size'] -= 1
    return my_list

def remove_first(my_list):
    """
    Elimina el primer elemento de la lista (single_linked_list) y disminuye
    el tamaño de la lista en 1. Retorna la información (info) del nodo eliminado.
    Si la lista está vacía, lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 2,
                            'first': <nodo1>,
                            'last': <nodo2>
                        }
    Returns:
        any: El valor (info) del nodo que se ha eliminado de la lista.
    Raises:
        IndexError: Si la lista está vacía.
    """
    if my_list['size'] == 0:
        raise IndexError("list index out of range")
    node_to_remove = my_list['first']
    info_removed = get_node_info(node_to_remove)
    my_list['first'] = node_to_remove['next']
    my_list['size'] -= 1
    if my_list['size'] == 0:
        my_list['last'] = None
    return info_removed

def remove_last(my_list):
    """
    Elimina el último elemento de la lista (single_linked_list) y disminuye
    el tamaño de la lista en 1. Retorna la información (info) del nodo eliminado.
    Si la lista está vacía, lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
    Returns:
        any: El valor (info) del nodo que se ha eliminado de la lista.
    Raises:
        IndexError: Si la lista está vacía.
    """
    if my_list['size'] == 0:
        raise IndexError("list index out of range")
    if my_list['size'] == 1:
        info_removed = get_node_info(my_list['first'])
        my_list['first'] = None
        my_list['last'] = None
        my_list['size'] = 0
        return info_removed
    current = my_list['first']
    while current['next'] != my_list['last']:
        current = current['next']
    info_removed = get_node_info(my_list['last'])
    current['next'] = None
    my_list['last'] = current
    my_list['size'] -= 1
    return info_removed

def insert_element(my_list, element, pos):
    """
    Inserta un elemento en la posición 'pos' de la lista (single_linked_list).
    Aumenta el tamaño de la lista en 1.
    Si la posición es inválida (pos < 0 o pos > my_list['size']), 
    lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 2,
                            'first': <nodo1>,
                            'last': <nodo2>
                        }
        element (any): Elemento a insertar en la lista.
        pos (int): Posición en la cual se insertará el elemento.
    Returns:
        dict: La lista (single_linked_list) actualizada.
    Raises:
        IndexError: Si la posición es inválida.
    """
    if pos < 0 or pos > my_list['size']:
        raise IndexError("list index out of range")
    new_node = new_single_node(element)
    if pos == 0:
        new_node['next'] = my_list['first']
        my_list['first'] = new_node
        if my_list['size'] == 0:
            my_list['last'] = new_node
    elif pos == my_list['size']:
        my_list['last']['next'] = new_node
        my_list['last'] = new_node
    else:
        current = my_list['first']
        for _ in range(pos - 1):
            current = current['next']
        new_node['next'] = current['next']
        current['next'] = new_node
    my_list['size'] += 1
    return my_list

def default_function(element_1, element_2):
    """
    Función de comparación por defecto.
    Retorna:
      0 si element_1 == element_2
      1 si element_1 > element_2
     -1 si element_1 < element_2
    Parameters:
        element_1 (any): Primer elemento a comparar.
        element_2 (any): Segundo elemento a comparar.
    Returns:
        int: El resultado de la comparación (1, 0, -1).
    """
    if element_1 > element_2:
        return 1
    elif element_1 < element_2:
        return -1
    else:
        return 0

def is_present(my_list, element, cmp_function):
    """
    Verifica si un elemento está presente en la lista.
    Para comparar los elementos, se utiliza la función de comparación cmp_function. 
    Si el elemento está presente, retorna su posición, en caso contrario retorna -1.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
        element (any): Elemento a buscar en la lista.
        cmp_function (function): Función de comparación que recibe dos elementos y
                                 retorna:
                                    - 0 si son iguales
                                    - 1 si el primero es mayor
                                    - -1 si el primero es menor
    Returns:
        int: Posición del elemento si está presente, -1 en caso contrario.
    """
    current = my_list['first']
    pos = 0
    while current is not None:
        current_info = get_node_info(current)
        if cmp_function(current_info, element) == 0:
            return pos
        current = current['next']
        pos += 1
    return -1

def change_info(my_list, pos, new_info):
    """
    Cambia la información de un elemento en la posición 'pos' por 'new_info'.
    Si la posición es inválida, lanza un IndexError: "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
        pos (int): Posición del elemento a modificar.
        new_info (any): Nueva información para reemplazar el elemento en 'pos'.
    Returns:
        dict: La lista (single_linked_list) actualizada.
    Raises:
        IndexError: Si la posición es inválida (pos < 0 o pos >= my_list['size']).
    """
    if pos < 0 or pos >= my_list['size']:
        raise IndexError("list index out of range")
    current = my_list['first']
    for _ in range(pos):
        current = current['next']
    current['info'] = new_info
    return my_list

def exchange(my_list, pos_1, pos_2):
    """
    Intercambia la información de dos elementos en las posiciones pos_1 y pos_2.
    Si alguna de las posiciones es inválida (pos_1 < 0, pos_2 < 0,
    pos_1 >= my_list['size'] o pos_2 >= my_list['size']), lanza un IndexError:
    "list index out of range".
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 3,
                            'first': <nodo1>,
                            'last': <nodo3>
                        }
        pos_1 (int): Posición del primer elemento a intercambiar.
        pos_2 (int): Posición del segundo elemento a intercambiar.
    Returns:
        dict: La lista (single_linked_list) con la información de los elementos intercambiada.
    Raises:
        IndexError: Si pos_1 o pos_2 es inválido.
    """
    size = my_list['size']
    if pos_1 < 0 or pos_1 >= size or pos_2 < 0 or pos_2 >= size:
        raise IndexError("list index out of range")
    if pos_1 == pos_2:
        return my_list
    if pos_1 > pos_2:
        pos_1, pos_2 = pos_2, pos_1
    current = my_list['first']
    node_1 = None
    node_2 = None
    for i in range(pos_2 + 1):
        if i == pos_1:
            node_1 = current
        if i == pos_2:
            node_2 = current
        current = current['next']
    info_1 = get_node_info(node_1)
    info_2 = get_node_info(node_2)
    node_1['info'] = info_2
    node_2['info'] = info_1
    return my_list

def sub_list(my_list, pos, num_elements):
    """
    Retorna una sublista de la lista original que inicia en la posición pos
    y contiene num_elements elementos.
    Si pos < 0 o pos >= my_list['size'], lanza IndexError: "list index out of range".
    Si pos + num_elements > my_list['size'], también lanza IndexError, 
    pues no hay suficientes elementos para formar la sublista solicitada.
    Parameters:
        my_list (dict): Estructura single_linked_list con llaves 'size', 'first', 'last'.
                        Ejemplo:
                        {
                            'size': 4,
                            'first': <nodo1>,
                            'last': <nodo4>
                        }
        pos (int): Posición inicial de la sublista.
        num_elements (int): Número de elementos a tomar desde pos.
    Returns:
        dict: Un nuevo single_linked_list con la sublista solicitada.
    Raises:
        IndexError: Si la posición inicial es inválida o 
                    si pos + num_elements excede el tamaño de la lista.
    """
    if pos < 0 or pos >= my_list['size']:
        raise IndexError("list index out of range")
    if pos + num_elements > my_list['size']:
        raise IndexError("list index out of range")
    new_sublist = new_list()
    current = my_list['first']
    for _ in range(pos):
        current = current['next']
    for _ in range(num_elements):
        info = get_node_info(current)
        add_last(new_sublist, info)
        current = current['next']
    return new_sublist

def default_sort_criteria(element_1, element_2):
   is_sorted = False
   if element_1 < element_2:
      is_sorted = True
   return is_sorted

def selection_sort(lst, sort_crit):
    """
    Ordena la lista 'lst' usando el algoritmo Selection Sort.
    
    Parámetros:
      lst: Lista a ordenar. Debe ser un diccionario con la estructura
           {'size': int, 'first': ..., 'last': ..., 'type': 'array_list' o 'single_linked_list'}.
      sort_crit: Función de comparación que recibe dos elementos y retorna True
                 si el primer elemento es menor que el segundo.
    
    Retorna:
      La misma lista 'lst', ordenada de forma ascendente según sort_crit.
    
    Ejemplo de uso:
      def less_than(a, b):
          return a < b
      sorted_lst = selection_sort(my_list, less_than)
    """
    n = size(lst)
    for i in range(n - 1):
        minimum = i
        for j in range(i + 1, n):
            if sort_crit(get_element(lst, j), get_element(lst, minimum)):
                minimum = j
        exchange(lst, i, minimum)
    return lst

def insertion_sort(lst, sort_crit):
    """
    Ordena la lista 'lst' usando el algoritmo Insertion Sort.
    
    Parámetros:
      lst: Lista a ordenar. Debe ser un diccionario con la estructura:
           {'size': int, 'first': ..., 'last': ..., 'type': 'array_list' o 'single_linked_list'}.
      sort_crit: Función de comparación que recibe dos elementos y retorna True
                 si el primer elemento es menor que el segundo.
    
    Retorna:
      La misma lista 'lst', ordenada de forma ascendente según sort_crit.
    
    Ejemplo de uso:
      def less_than(a, b):
          return a < b
      sorted_lst = insertion_sort(my_list, less_than)
    """
    n = size(lst)
    for i in range(1, n):
        j = i
        while j > 0 and sort_crit(get_element(lst, j), get_element(lst, j-1)):
            exchange(lst, j, j-1)
            j -= 1
    return lst

def shell_sort(lst, sort_crit):
    """
    Ordena la lista 'lst' usando el algoritmo Shell Sort.

    Parámetros:
      lst: Lista a ordenar. Debe ser un diccionario con la estructura:
           {'size': int, 'first': ..., 'last': ..., 'type': 'array_list' o 'single_linked_list'}.
      sort_crit: Función de comparación que recibe dos elementos y retorna True
                 si el primer elemento es menor que el segundo (criterio de orden ascendente).

    Retorna:
      La misma lista 'lst', ordenada de forma ascendente según sort_crit.

    Ejemplo de uso:
      def less_than(a, b):
          return a < b
      sorted_lst = shell_sort(my_list, less_than)
    """    
    n = size(lst)
    h = 1
    while h < n/3:
        h = 3 * h + 1
    while h >= 1:
        for i in range(h, n):
            j = i
            while j >= h and sort_crit(get_element(lst, j), get_element(lst, j - h)):
                exchange(lst, j, j - h)
                j -= h
        h //= 3
    return lst

def merge_sort(lst, sort_crit):
    """
    Ordena la lista 'lst' usando el algoritmo Merge Sort.

    Parámetros:
      lst: Lista a ordenar. Debe ser un diccionario con la estructura
           {'size': int, 'first': ..., 'last': ..., 'type': 'array_list' o 'single_linked_list'}.
      sort_crit: Función de comparación que recibe dos elementos y retorna True
                 si el primer elemento es menor que el segundo (criterio de ordenación ascendente).

    Retorna:
      La misma lista 'lst', ordenada de forma ascendente según sort_crit.

    Ejemplo de uso:
      def less_than(a, b):
          return a < b
      sorted_lst = merge_sort(my_list, less_than)
    """
    n = size(lst)
    if n > 1:
        mid = n // 2
        left_list = sub_list(lst, 0, mid)
        right_list = sub_list(lst, mid, n - mid)
        merge_sort(left_list, sort_crit)
        merge_sort(right_list, sort_crit)
        i = j = k = 0
        left_size = size(left_list)
        right_size = size(right_list)
        while i < left_size and j < right_size:
            if sort_crit(get_element(right_list, j), get_element(left_list, i)):
                change_info(lst, k, get_element(right_list, j))
                j += 1
            else:
                change_info(lst, k, get_element(left_list, i))
                i += 1
            k += 1
        while i < left_size:
            change_info(lst, k, get_element(left_list, i))
            i += 1
            k += 1
        while j < right_size:
            change_info(lst, k, get_element(right_list, j))
            j += 1
            k += 1
    return lst

def quick_sort(lst, sort_crit):
    """
    Ordena la lista 'lst' usando el algoritmo Quick Sort.
    
    Parámetros:
      lst: Lista a ordenar. Debe ser un diccionario con la estructura
           {'size': int, 'first': ..., 'last': ..., 'type': 'array_list' o 'single_linked_list'}.
      sort_crit: Función de comparación que recibe dos elementos y retorna True
                 si el primer elemento es menor que el segundo.
                 
    Retorna:
      La misma lista 'lst', ordenada de forma ascendente según sort_crit.
      
    Ejemplo de uso:
      def less_than(a, b):
          return a < b
      sorted_lst = quick_sort(lst, less_than)
    """
    def partition(lo, hi):
        """
        Reorganiza los elementos entre los índices lo y hi usando el elemento en hi como pivot.
        Devuelve la posición final del pivot.
        """
        follower = lo
        pivot = get_element(lst, hi)
        for leader in range(lo, hi):
            if sort_crit(get_element(lst, leader), pivot):
                exchange(lst, follower, leader)
                follower += 1
        exchange(lst, follower, hi)
        return follower
    def quicksort(lo, hi):
        if lo < hi:
            pivot_index = partition(lo, hi)
            quicksort(lo, pivot_index - 1)
            quicksort(pivot_index + 1, hi)
    n = size(lst)
    if n > 0:
        quicksort(0, n - 1)
    return lst