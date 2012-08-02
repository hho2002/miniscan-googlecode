# -*- coding: gb2312 -*-
from bottle import route, run, get, post, request, response, redirect 
from bottle import template, view, static_file

demo_tasks={
        'task1':{'name':'task1', 'status':"run", 'id':1, 'ref':1, 'log_count':10, 'remain':30, 'current':"172.16.16.1"},
        'task2':{'name':'task2', 'status':"pause", 'id':2, 'ref':2, 'log_count':11, 'remain':40, 'current':"172.16.16.99"}
       }

@route('/static/<filename:path>')
def send_static(filename):
    print filename
    return static_file(filename, root='./static/')

@route('/ajax')
def do_ajax():
    
    # http://docs.python.org/library/json.html
    import json
    
    action = request.query.action
    timestamp = request.query.timestamp
    print "do_ajax:", action, timestamp
    
    demo_tasks['task1']['log_count'] += 1
    
    return json.dumps(demo_tasks)

@route('/submit_task', method='POST')
def do_submit_task():
    name = request.forms.task_name
    data = request.files.task_data
    if name and data and data.file:
        raw = data.file.read() # This is dangerous for big files
        filename = data.filename
        return "task_name:%s! \ntask_data:%s (%d bytes)\n%s\nsuccess!" % (name, filename, len(raw), raw)
    return "do_submit_task: missed a field."

def check_login(name, password):
    print "check_login", name, password
    return True

@get('/login') # or @route('/login')
@view('login')
def login_form():
    return {}

@post('/login') # or @route('/login', method='POST')
def login_submit():
    username = request.forms.name
    password = request.forms.password
    if check_login(username, password):
        response.set_cookie("account", username, secret='812ho012')        
        redirect("/")
    else:
        return "<p>Login failed</p>"

@get('/logout')
def logout_submit():
    response.delete_cookie("account")
    redirect("/")

@route('/')
@view('main_template')
def main_view():
    username = request.get_cookie("account", secret='812ho012')
    if username:
        return dict(username=username, tasks=demo_tasks)
    else:
        redirect("/login")

if __name__ == '__main__':
    run(host='localhost', port=80)
    