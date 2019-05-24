from flask import Blueprint

auths = Blueprint('auth', __name__, url_prefix='/auth')

from . import views, api
from .auth import basic_auth, token_auth, multi_auth