# A very simple Bottle Hello World app for you to get started with...
import datetime
import time
import os
import dataset

db = dataset.connect('sqlite:///todo.db')

from bottle import get, post, request, response, template, redirect, default_app


# APPLICATION PAGES AND ROUTES

# assume collection contains fields "id", "task", "status"

@get('/')
def get_show_list():
    result = db['todo'].all()
    # get all the items in the collection as a list of dictionaries
    return template("show_list", rows=[dict(r) for r in result], session={})

@get('/update_status/<id:int>/<value:int>')
def get_update_status(id, value):
    #result = db['todo'].find_one(id=id)
    # given an id, find the relevant document and update the status value
    db['todo'].update({'id':id, 'status':(value!=0)},['id'])
    redirect('/')


@get('/delete_item/<id:int>')
def get_delete_item(id):
    # given an id, delete the relevant document
    db['todo'].delete(id=id)
    redirect('/')


@get('/update_task/<id:int>')
def get_update_task(id):
    # given an id, get the document and populate a form
    result = db['todo'].find_one(id=id)
    return template("update_task", row=dict(result))


@post('/update_task')
def post_update_task():
    # given an id and an updated task in a form, find the document and modify the task value
    id = int(request.forms.get("id").strip())
    updated_task = request.forms.get("updated_task").strip()
    db['todo'].update({'id':id, 'task':updated_task},['id'])
    redirect('/')


@get('/new_item')
def get_new_item():
    return template("new_item")


@post('/new_item')
def post_new_item():
    # given a new task in a form, create and insert a document and with that task value
    new_task = request.forms.get("new_task").strip()
    db['todo'].insert({'task':new_task, 'status':False})
    redirect('/')

application = default_app()

if __name__ == "__main__":
    print(get_show_list())
