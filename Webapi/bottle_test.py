# from bottle import route, run, template

# @route('/hello/<name>')
# def index(name):
#     return template('<b>Hello {{name}}</b>!', name=name)

# run(host='127.0.0.1', port=8080)


# from bottle import Bottle, run

# app = Bottle()

# @app.route('/hello')
# def hello():
#     test_in = input("Log in something:")
#     outcome = f"{test_in} is really good!"
#     return outcome

# run(app, host='localhost', port=8080)


from bottle import get, post, request,run # or route

def check_login(username, password):
    return True


@get('/login') # or @route('/login')
def login():
    return '''
        <form action="/login" method="post">
            Username: <input name="username" type="text" />
            Password: <input name="password" type="password" />
            <input value="Login" type="submit" />
        </form>
    '''


@post('/login') # or @route('/login', method='POST')
def do_login():
    username = request.forms.get('username')
    password = request.forms.get('password')
    if check_login(username, password):
        return "<p>Your login information was correct.</p>"
    else:
        return "<p>Login failed.</p>"

run(host='0.0.0.0', port=8080)