import os
from flask import Flask, request, g, make_response
from flask_pymongo import PyMongo
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from app import config
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from flask_uploads import UploadSet, configure_uploads, IMAGES, ALL
from app.common import ObjectIdEncoder

mongo = PyMongo()
db = SQLAlchemy()
serializer = Serializer(config.SECRET_KEY, expires_in=43200)
photos = UploadSet('PHOTO')

from app.auth.auth import basic_auth, multi_auth, token_auth

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config_name)
    app.config['UPLOADED_PHOTO_DEST'] = os.path.join(app.root_path, 'static', 'media')
    app.config['UPLOADED_PHOTO_ALLOW'] = IMAGES

    app.json_encoder = ObjectIdEncoder # 自定义json转化类，解决jsonify对ObjectID的转化问题

    db.init_app(app)

    mongo.init_app(app)
    configure_uploads(app, photos)
    CORS(app)

    from app.auth import auths
    app.register_blueprint(auths)

    from app.zmms import zmms
    app.register_blueprint(zmms)

    from app.scada import scada
    app.register_blueprint(scada)

    return app