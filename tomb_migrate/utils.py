from os import listdir, mkdir
from os.path import isfile, isdir, join, basename
from importlib.machinery import SourceFileLoader
from functools import partial
from datetime import datetime

# TODO: This should be optional dependency
import psycopg2
import ujson
import click
import sys

from psycopg2.extras import register_default_jsonb
from psycopg2.extras import Json as pjson
Json = partial(pjson, dumps=ujson.dumps)


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


class AlreadyInitializedException(Exception):
    pass


class NoMigrationsFoundException(Exception):
    pass


class EngineContainer:
    # TODO: This should be based on entrypoints
    type_map = {
        'postgresql': _get_psyco_engine
    }

    def __init__(self, name, settings):
        self.name = name
        self.settings = settings
        self.type = settings['type']
        self.host = settings['host']

        if self.type in self.type_map:
            self.engine = self.type_map[self.type](settings)
        else:
            raise Exception("Unknown database type: %s" % settings['type'])

    def _init_psyco(self):
        create_sql = """CREATE TABLE IF NOT EXISTS tomb_migrate_version(
            version int NOT NULL,
            date_updated timestamp)"""
        insert_sql = """INSERT INTO tomb_migrate_version(version, date_updated)
        VALUES(%s, %s)"""

        select_sql = "SELECT * FROM tomb_migrate_version"

        with self.engine.cursor() as curs:
            curs.execute(create_sql)
            curs.execute(select_sql)
            result = curs.fetchall()

            if len(result) > 0:
                raise AlreadyInitializedException()

            curs.execute(insert_sql, (0, datetime.utcnow()))
            self.engine.commit()

    def _update_psyco(self, version):
        update_sql = """UPDATE tomb_migrate_version
                        SET version=%s,
                            date_updated=%s"""

        with self.engine.cursor() as curs:
            curs.execute(update_sql, (version, datetime.utcnow()))
            self.engine.commit()

    def initialize_marker(self):
        # TODO: This should be based on entrypoints
        type_map = {
            'postgresql': self._init_psyco
        }

        if self.type in type_map:
            type_map[self.type]()
        else:
            raise Exception('Unknown database type: %s' % self.type)

    def update_revision(self, version):
        # TODO: This should be based on entrypoints
        type_map = {
            'postgresql': self._update_psyco
        }
        if self.type in type_map:
            type_map[self.type](version)
        else:
            raise Exception('Unknown database type %s' % self.type)

    def __unicode__(self):
        return '%s (%s)' % (self.name, self.host)

    __str__ = __unicode__


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
    if not isdir(directory):
        mkdir(directory)

    files = []
    for f in listdir(directory):
        path = join(directory, f)
        if not isfile(path):
            continue

        files.append(Revision(path))

    if not files:
        raise NoMigrationsFoundException()

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
    engines = {}

    for name, db in settings.items():
        engines[name] = EngineContainer(name, db)

    return engines


def create_new_revision(directory, message):
    try:
        current_revisions = get_upgrade_path(directory)
        current_version = current_revisions[-1].version
    except NoMigrationsFoundException:
        current_version = 1

    padded_version = "{0:04d}".format(current_version)
    tmpl = """\
import click

def upgrade(engine):
    click.echo('Run upgrade!')

def downgrade(engine):
    click.echo('Run downgrade!')
"""
    description = message.replace(' ', '_')
    fname = '%s_%s.py' % (padded_version, description)

    path = join(directory, fname)

    with open(path, "w") as revision_file:
        revision_file.write(tmpl)

    return path
