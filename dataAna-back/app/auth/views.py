from flask import jsonify, g
from app.auth import auths
from .auth import multi_auth, basic_auth
from app import mongo, serializer


@multi_auth.login_required
@auths.route('/test')
def hello():
    #from run import app
    # print(app.root_path)
    #print(serializer.dumps({'username': 'ww'}))
    return 'hello,world'


@auths.route('/test2')
@multi_auth.login_required
def hello2():
    return "Hello, %s,%s!" % (basic_auth.username(), g.user)
