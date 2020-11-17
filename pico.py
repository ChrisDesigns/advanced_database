import pickle
import os

items = [ ]

filename = None

def everything(v):
    return v

def always(v):
    return True

def open_database(name):
    global filename
    global items
    filename = name
    if os.path.exists(filename):
        with open(filename, "rb") as f:
            items = pickle.load(f)
    else:
        items = [ ]

def commit():
    global filename
    global items
    with open(filename, "wb") as f:
        pickle.dump(items,f)

def insert(data):
    items.append(data)
    commit()

def query(select, where):
    return [select(item) for item in items if where(item)]

if __name__ == "__main__":
    open_database("pico.pkl")
    # insert({"id":1, "task":"do something useful", "status":0})
    # insert({"id":2, "task":"do something else useful", "status":0})
    #insert({"id":3, "task":"do something enjoyable", "status":0})
    select = lambda v : v
    where = lambda v : True
    for item in query(select=everything, where=always):
        print(item)
    for item in query(select=lambda v:v['task'], where=lambda v:v['id']==3):
        print(item)



