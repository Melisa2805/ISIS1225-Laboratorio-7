def iterator(my_list):
    """
    Función generadora que permite iterar sobre una lista de tipo array_list o single_linked_list.
    Permite usar: for i in iterator(lista)

    Parameters:
        my_list (dict): Lista, ya sea de tipo array_list o single_linked_list.

    Yields:
        any: Elemento de la lista en cada iteración.
    """
    list_type = my_list.get('type', None)
    if list_type == 'array_list':
        for element in my_list['elements']:
            yield element
    elif list_type == 'single_linked_list':
        current = my_list['first']
        while current is not None:
            yield current['info']
            current = current['next']
    else:
        raise TypeError("Tipo de lista no soportado")
