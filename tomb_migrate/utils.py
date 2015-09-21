from os import listdir, mkdir
from os.path import isfile, isdir, join, basename
from importlib.machinery import SourceFileLoader
from functools import partial
from datetime import datetime
from abc import ABCMeta, abstractmethod

# TODO: This should be optional dependency
import psycopg2
import rapidjson
import pkg_resources
import rethinkdb
import pytz

from psycopg2.extras import register_default_jsonb
from psycopg2.extras import Json as pjson
Json = partial(pjson, dumps=rapidjson.dumps)
UTC = pytz.utc
MARKER_TABLE_NAME = 'tomb_migrate_version'


class NotInitializedException(Exception):
    pass


class AlreadyInitializedException(Exception):
    pass


class NoMigrationsFoundException(Exception):
    pass


class InvalidMigrationFileName(Exception):
    pass


class UnknownDatabaseType(Exception):
    pass


def utc_now():
    now = datetime.utcnow()
    tz_now = now.replace(tzinfo=UTC)
    return tz_now


def get_revision_from_name(filename):
    try:
        name_without_path = basename(filename)
        rev, info = name_without_path.split('_', 1)
        info = info.split('.')[:-1][0]
        revision = int(rev)
        description = ' '.join(info.split('_'))
        return revision, description
    except:
        raise InvalidMigrationFileName(
            "%s is not a valid migration file" % filename
        )


class BaseDatabaseContainer:
    __metaclass__ = ABCMeta

    def __init__(self, name, settings):
        self.name = name
        self.settings = settings
        self.type = settings['type']
        self.host = settings['host']

    @abstractmethod
    def init(self):
        raise NotImplementedError()

    @abstractmethod
    def update(self, version):
        raise NotImplementedError()

    @abstractmethod
    def current_version(self):
        raise NotImplementedError()

    def __unicode__(self):
        return '%s (%s)' % (self.name, self.host)

    __str__ = __unicode__


class RethinkDBContainer(BaseDatabaseContainer):
    def __init__(self, name, settings):
        super().__init__(name, settings)

        kwargs = {
            'host': settings['host'],
            'db': settings['database'],
        }

        optional_keys = [
            'port'
        ]

        for key in optional_keys:
            if key in settings:
                kwargs[key] = settings[key]

        self.conn = rethinkdb.connect(**kwargs)

    def init(self):
        current_version = self.current_version()

        if current_version is not None:
            raise AlreadyInitializedException()

        rethinkdb.table_create(MARKER_TABLE_NAME).run(self.conn)
        rethinkdb.table(MARKER_TABLE_NAME).insert({
            'version': 0,
            'date_updated': utc_now(),
        }).run(self.conn)

    def update(self, version):
        try:
            row = list(rethinkdb.table(MARKER_TABLE_NAME).run(self.conn))[0]
            row.update({
                'version': version,
                'date_updated': utc_now()
            })
            rethinkdb.table(MARKER_TABLE_NAME).update(row).run(self.conn)
        except rethinkdb.errors.ReqlOpFailedError as e:
            msg = 'Database `%s` does not exist.' % self.settings['database']
            if e.message == msg:
                raise NotInitializedException()
            raise

    def current_version(self):
        try:
            result = list(rethinkdb.table(MARKER_TABLE_NAME).run(self.conn))
        except rethinkdb.errors.ReqlOpFailedError as e:
            msg = 'Table `%s.%s` does not exist.' % (
                self.settings['database'],
                MARKER_TABLE_NAME
            )
            if msg == e.message:
                return None
            raise

        return result[0]['version']


class PsycoDBContainer(BaseDatabaseContainer):
    def __init__(self, name, settings):
        super().__init__(name, settings)

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
        register_default_jsonb(conn, loads=rapidjson.loads)

        self.conn = conn

    def current_version(self):
        select_sql = "SELECT * FROM %s LIMIT 1" % MARKER_TABLE_NAME

        with self.conn.cursor() as curs:
            curs.execute(select_sql)
            result = curs.fetchone()
            return result[0]

    def init(self):
        create_sql = """CREATE TABLE IF NOT EXISTS %s(
            version int NOT NULL,
            date_updated timestamp)""" % MARKER_TABLE_NAME
        insert_sql = """INSERT INTO {0}(version, date_updated)
                     VALUES(%s, %s)""".format(MARKER_TABLE_NAME)

        current_version = self.current_version()

        if current_version is not None:
            raise AlreadyInitializedException()

        with self.conn.cursor() as curs:
            curs.execute(create_sql)
            curs.execute(insert_sql, (0, utc_now()))
            self.conn.commit()

    def update(self, version):
        update_sql = """UPDATE {0}
                        SET version=%s,
                            date_updated=%s""".format(MARKER_TABLE_NAME)

        with self.conn.cursor() as curs:
            try:
                curs.execute(update_sql, (version, datetime.utcnow()))
                self.conn.commit()
            except psycopg2.ProgrammingError as e:
                if e.pgcode == "42P01":
                    raise NotInitializedException()
                raise


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

    Optionals `version` argument if you want to start from
    a certain location.
    """
    revisions = get_files_in_directory(directory)
    if version:
        revisions_to_run = [r for r in revisions if r.version >= version]
    else:
        revisions_to_run = revisions

    return revisions_to_run


def get_downgrade_path(directory, version=None):
    """
    Loads all the files in the order necessary to downgrade.

    Optionals `revision` argument if you want to start from
    a certain location.
    """
    revisions = reversed(get_files_in_directory(directory))
    if version:
        revisions_to_run = [r for r in revisions if r.version <= version]
    else:
        revisions_to_run = revisions

    return revisions_to_run


def get_databases_from_settings(settings):
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
    providers = {}
    databases = {}


    for ep in pkg_resources.iter_entry_points('tomb_migrate.db_providers'):
        name = ep.name
        db_module = ep.load()
        providers[name] = db_module

    for name, db in settings.items():
        provider = providers.get(db['type'])

        if provider is None:
            raise UnknownDatabaseType(db['type'])

        databases[name] = provider(name, db)

    return databases


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
