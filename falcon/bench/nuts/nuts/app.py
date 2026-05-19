from pecan import make_app

# from .controllers import root


def create():
    return make_app('controllers.root.RootController', logging={}, debug=False)


