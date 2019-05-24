from flask import Blueprint

zmms = Blueprint('zmms', __name__, url_prefix='/zmms')

from . import views, api