import random as rd
from DataStructures.List import array_list as al
from DataStructures.List import single_linked_list as sll
from DataStructures.List.list_iterator import iterator
from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_entry as me

def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(int(num_elements / load_factor))
    scale = rd.randint(1, prime - 1)
    shift = rd.randint(0, prime - 1)
    table = al.new_list()
    for _ in range(capacity):
        table = al.add_last(table, sll.new_list())
    hashtable = {
        "prime": prime,
        "capacity": capacity,
        "scale": scale,
        "shift": shift,
        "table": table,
        "current_factor": 0,
        "limit_factor": load_factor,
        "size": 0,
        "type": "SEPARATE_CHAINING"
    }
    return hashtable

def default_compare(entry, key):
    entry_key = me.get_key(entry)
    if entry_key == key:
        return 0
    elif entry_key > key:
        return 1
    else:
        return -1

def put(my_map, key, value):
    hash_code = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], hash_code)
    entry = me.new_map_entry(key, value)
    pos = sll.is_present(bucket, key, default_compare)
    if pos >= 0:
        sll.change_info(bucket, pos, entry)
    else:
        sll.add_last(bucket, entry)
        my_map["size"] += 1
        my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    if my_map["current_factor"] > my_map["limit_factor"]:
        rehash(my_map)
    return my_map 

def contains(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], hash_code)
    pos = sll.is_present(bucket, key, default_compare)
    return pos >= 0

def remove(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], hash_code)
    if bucket is not None:
        pos = sll.is_present(bucket, key, default_compare)
        if pos >= 0:
            sll.delete_element(bucket, pos)
            my_map["size"] -= 1
    return my_map 

def get(my_map, key):
    hash_code = mf.hash_value(my_map, key)
    bucket = al.get_element(my_map["table"], hash_code)
    pos = sll.is_present(bucket, key, default_compare)
    if pos >= 0:
        return me.get_value(sll.get_element(bucket, pos))
    return None

def size(my_map):
    return my_map["size"]

def is_empty(my_map):
    return my_map["size"] == 0

def key_set(my_map):
    keys = al.new_list()
    for bucket in iterator(my_map["table"]):
        for entry in iterator(bucket):
            keys = al.add_last(keys, me.get_key(entry))
    return keys

def value_set(my_map):
    values = al.new_list()
    for bucket in iterator(my_map["table"]):
        for entry in iterator(bucket):
            values = al.add_last(values, me.get_value(entry))
    return values

def rehash(my_map):
    capacity = mf.next_prime(my_map["capacity"] * 2)
    new_table = al.new_list()
    for _ in range(capacity):
         new_table = al.add_last(new_table, sll.new_list())
    old_table = my_map["table"]
    my_map["size"] = 0
    my_map["current_factor"] = 0
    my_map["table"] = new_table
    my_map["capacity"] = capacity
    for pos in range(al.size(old_table)):
         bucket = al.get_element(old_table, pos)
         for entry in iterator(bucket):
             key = me.get_key(entry)
             value = me.get_value(entry)
             put(my_map, key, value)
    return my_map
