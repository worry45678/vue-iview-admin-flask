from flask import Blueprint

scada = Blueprint('scada', __name__, url_prefix='/scada')

from . import views, api