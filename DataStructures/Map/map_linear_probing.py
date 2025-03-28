import random as rd
from DataStructures.List import array_list as al
from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_entry as me

def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(int(num_elements / load_factor))
    scale = rd.randint(1, prime - 1)
    shift = rd.randint(0, prime - 1)
    table = al.new_list()
    for _ in range(capacity):
        table = al.add_last(table, me.new_map_entry(None, None))
    hashtable = {
        "prime": prime,
        "capacity": capacity,
        "scale": scale,
        "shift": shift,
        "table": table,
        "current_factor": 0,
        "limit_factor": load_factor,
        "size": 0,
        "type": "LINEAR_PROBING"
    }
    return hashtable

def put(my_map, key, value):
    hash_code = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, hash_code)
    entry = al.get_element(my_map["table"], pos)
    if occupied:
        me.set_value(entry, value)
    else:
        me.set_key(entry, key)
        me.set_value(entry, value)
        my_map["size"] += 1
    my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    if my_map["current_factor"] > my_map["limit_factor"]:
        rehash(my_map)
    return my_map

def find_slot(my_map, key, hash_value):
    first_avail = None
    found = False
    occupied = False
    while not found:
        if is_available(my_map["table"], hash_value):
            if first_avail is None:
                first_avail = hash_value
            entry = al.get_element(my_map["table"], hash_value)
            if me.get_key(entry) is None:
                found = True  # Se encontró un slot vacío
        elif default_compare(key, al.get_element(my_map["table"], hash_value)) == 0:
            first_avail = hash_value
            found = True
            occupied = True
        hash_value = (hash_value + 1) % my_map["capacity"]
    return occupied, first_avail

def is_available(table, pos):
    entry = al.get_element(table, pos)
    if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
        return True
    return False

def default_compare(key, entry):
    if key == me.get_key(entry):
        return 0
    elif key > me.get_key(entry):
        return 1
    return -1

def contains(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, hash_code)
    return occupied

def remove(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, hash_code)
    if occupied:
        entry = me.new_map_entry('__EMPTY__', '__EMPTY__')
        my_map["table"] = al.change_info(my_map["table"], pos, entry)
        my_map["size"] -= 1
    return my_map

def get(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, hash_code)
    if occupied:
        entry = al.get_element(my_map["table"], pos)
        return me.get_value(entry)
    return None

def size(my_map):
    return my_map["size"]

def is_empty(my_map):
    return my_map["size"] == 0

def key_set(my_map):
    keys = al.new_list()
    for pos in range(al.size(my_map["table"])):
        entry = al.get_element(my_map["table"], pos)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            keys = al.add_last(keys, me.get_key(entry))
    return keys

def value_set(my_map):
    values = al.new_list()
    for pos in range(al.size(my_map["table"])):
        entry = al.get_element(my_map["table"], pos)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            values = al.add_last(values, me.get_value(entry))
    return values

def rehash(my_map):
    new_table = al.new_list()
    capacity = mf.next_prime(my_map["capacity"] * 2)
    for _ in range(capacity):
        new_table = al.add_last(new_table, me.new_map_entry(None, None))
    old_table = my_map["table"]
    my_map["size"] = 0
    my_map["current_factor"] = 0
    my_map["table"] = new_table
    my_map["capacity"] = capacity
    for pos in range(al.size(old_table)):
        entry = al.get_element(old_table, pos)
        if me.get_key(entry) is not None and me.get_key(entry) != "__EMPTY__":
            hash_code = mf.hash_value(my_map, me.get_key(entry))
            occupied, slot = find_slot(my_map, me.get_key(entry), hash_code)
            my_map["table"] = al.change_info(my_map["table"], slot, entry)
            my_map["size"] += 1
            my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    return my_map
