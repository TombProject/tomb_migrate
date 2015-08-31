from os import listdir
from os.path import isfile, join, basename
from importlib.machinery import SourceFileLoader
from functools import partial

# TODO: This should be optional dependency
import psycopg2
import ujson
from psycopg2.extras import register_default_jsonb
from psycopg2.extras import Json as pjson
Json = partial(pjson, dumps=ujson.dumps)


def get_revision_from_name(filename):
    try:
        name_without_path = basename(filename)
        rev, info = name_without_path.split('_', 1)
        info = info.split('.')[:-1][0]
        revision = int(rev)
        description = ' '.join(info.split('_'))
        return revision, description
    except:
        raise Exception("%s is not a valid migration file" % filename)


class Revision:
    def __init__(self, filename):
        self.filename = filename
        rev, description = get_revision_from_name(filename)
        self.version = rev
        self.description = description
        module = SourceFileLoader(filename, filename).load_module()
        self.upgrade = module.upgrade
        self.downgrade = module.downgrade

    def __repr__(self):
        return '<Revision: version=%s, desc=%s>' % (
            self.version,
            self.description
        )

    def __eq__(self, other):
        # TODO: Maybe be smarter about this?
        r1 = repr(self)
        r2 = repr(other)

        return r1 == r2


def get_files_in_directory(directory):
    """
    Get all file in a directory, exclude any directories. This will sort by
    revision number.
    """
    files = []
    for f in listdir(directory):
        path = join(directory, f)
        if not isfile(path):
            continue

        files.append(Revision(path))

    revisions = sorted(files, key=lambda r: r.version)
    return revisions


def get_upgrade_path(directory, version=None):
    """
    Loads all the files in the order necessary to upgrade.

    Optionals `revision` argument if you want to start from
    a certain location.
    """
    revisions = get_files_in_directory(directory)
    if version:
        revisions_to_run = [r for r in revisions if r.version >= version]
    else:
        revisions_to_run = revisions

    return revisions_to_run


def _get_psyco_engine(settings):
    kwargs = {
        'host': settings['host'],
        'database': settings['database'],
    }
    optional_keys = [
        'port', 'username', 'password'
    ]

    for key in optional_keys:
        if key in settings:
            kwargs[key] = settings[key]

    conn = psycopg2.connect(**kwargs)
    register_default_jsonb(conn, loads=ujson.loads)

    return conn


def get_engines_from_settings(settings):
    """
    This gets database engines for each db in settings.

    Settings should look like:

    .. code-block:: python

        {
            'name': {
                'type': 'postgresql'
                'host': '127.0.0.1',
                'port': '1337',
                'database': 'test'
            }
         }
    """
    # TODO: This should be based on entrypoints
    type_map = {
        'postgresql': _get_psyco_engine
    }
    engines = {}
    for name, db in settings.items():
        db_type = db['type']
        if db_type in type_map:
            engine = type_map[db_type](db)
            engines[name] = engine
        else:
            raise Exception("Unknown database type: %s" % db['type'])

    return engines
