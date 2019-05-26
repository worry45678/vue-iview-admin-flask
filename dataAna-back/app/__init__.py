import os
from flask import Flask, request, g, make_response
from flask_pymongo import PyMongo
from flask_cors import CORS
from app import config
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from app.common import ObjectIdEncoder

mongo = PyMongo()
serializer = Serializer(config.SECRET_KEY, expires_in=43200)

from app.auth.auth import basic_auth, multi_auth, token_auth

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config_name)

    app.json_encoder = ObjectIdEncoder # 自定义json转化类，解决jsonify对ObjectID的转化问题

    mongo.init_app(app)
    CORS(app)

    from app.auth import auths
    app.register_blueprint(auths)

    return app