from flask import jsonify, g
from . import zmms
from app.auth import multi_auth, basic_auth, token_auth
from app import mongo, serializer

@zmms.route('/test/')
def test():
    return 'welcome,zmms'
