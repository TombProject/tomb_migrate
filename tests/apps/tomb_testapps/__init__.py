from pyramid.config import Configurator


def simple(global_conf, **settings):
    config = Configurator(settings=settings)
    wsgi_app = config.make_wsgi_app()
    return wsgi_app
