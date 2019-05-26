
from flask import Blueprint

auths = Blueprint('auth', __name__)
from .auth import basic_auth, token_auth, multi_auth
from . import views, api