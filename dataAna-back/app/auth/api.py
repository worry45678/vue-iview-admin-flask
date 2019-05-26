from flask import jsonify, request, g
from . import auths
from app import mongo, serializer
from app.auth.auth import multi_auth
import json

@auths.route('/login', methods=['GET', 'POST'])
@multi_auth.login_required
def get_all_users():
    user = mongo.db.users.find_one({'name': g.user},{"_id":0,"token":0})
    token = serializer.dumps({'username': user['name']})
    #mongo.db.users.update({'name': 'ww'},{'$set':{'token': token}})
    return jsonify({'username': user['name'], 'token': token.decode('ascii'), 'code': 20000})

@auths.route('/get_info')
@multi_auth.login_required
def get_info():
    user_info = mongo.db.users.find_one({'name': g.user}, {'_id':0, 'pwd': 0, 'token':0})
    return jsonify(user_info)
    #return jsonify({'name': 'ww', 'user_id': '1', 'access': ['super_admin', 'admin'], 'avatar': 'https://file.iviewui.com/dist/a0e88e83800f138b94d2414621bd9704.png'})


@auths.route('/logout', methods=['POST'])
@multi_auth.login_required
def logout():
    # 返回未读消息数量
    return jsonify({'code': 20000, 'message': 'Logout'})

@auths.route('/message/count', methods=['GET'])
@multi_auth.login_required
def message_count():
    return jsonify(0)

@auths.route('/message/init', methods=['GET'])
@multi_auth.login_required
def message_init():
    return jsonify({'readed':[], 'trash':[], 'unread':[]})

@auths.route('/message/content/<string:id>')
@multi_auth.login_required
def message_content(id):
    return jsonify(f'id:{id}')

@auths.route('/message/has_read/<string:id>')
@multi_auth.login_required
def message_has_read(id):
    return jsonify(True)

@auths.route('/message/remove_readed/<string:id>')
@multi_auth.login_required
def message_remove_readed(id):
    return jsonify(True)

@auths.route('/message/restore/<string:id>')
@multi_auth.login_required
def message_restore(id):
    return jsonify(True)

@auths.route('/register', methods=['POST'])
def add_user():
    star = mongo.db.users
    name = request.form['name']
    pwd = request.form['pwd']
    star_id = star.insert({'name': name, 'pwd': pwd})
    new_star = star.find_one({'_id': star_id})
    output = {'name': new_star['name'], 'pwd': new_star['pwd']}
    return jsonify({'result': output})

@auths.route('/modify/<string:name>', methods=['PUT'])
def update_user(name):
    user = mongo.db.users.find_one({'name': name})
    new_name = request.form['name']
    mongo.db.users.update({'name': name}, {'$set':{'name': new_name}})
    return jsonify({'results': new_name})


@auths.route('/delete/<string:name>', methods=['DELETE'])
def delete_user(name):
    user = mongo.db.users.find_one({"name": name})
    mongo.db.users.remove({'name': name})
    return jsonify({'results': True})

@auths.route('/save_error_logger', methods=['POST'])
def save_error_logger():
    data = json.loads(request.data)
    print(data)
    return jsonify({'results': True})