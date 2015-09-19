import click
import os
import sys

from tomb_migrate.utils import get_engines_from_settings
from tomb_migrate.utils import get_upgrade_path, get_downgrade_path
from tomb_migrate.utils import create_new_revision

from tomb_migrate.utils import (
    AlreadyInitializedException,
    NoMigrationsFoundException,
    NotInitializedException,
)


@click.group(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '--path', '-p',
    type=click.Path(resolve_path=True),
    default='./db/',
    help='The path the migration files live'
)
@click.pass_context
def db(ctx, path):
    settings = ctx.obj.pyramid_env['registry'].settings
    db_settings = settings['databases']
    engines = get_engines_from_settings(db_settings)
    ctx.obj.db_engines = engines
    ctx.obj.db_path = os.path.abspath(path)


@db.command()
@click.pass_context
def upgrade(ctx):
    """
    Upgrade the database to revision
    """
    try:
        upgrade_path = get_upgrade_path(ctx.obj.db_path)
    except NoMigrationsFoundException:
        click.echo(
            "Did not find any migrations to run in %s" % ctx.obj.db_path
        )
        click.echo(
            "Have you tried running `tomb db revision -m <description>`?"
        )
        sys.exit(1)

    for revision in upgrade_path:
        for name, engine in ctx.obj.db_engines.items():
            click.echo('Running upgrade %s' % revision)
            try:
                revision.upgrade(engine)
                engine.update_revision(revision.version)
            except NotInitializedException:
                msg = (
                    "Upgraded was not completed!, Looks like %s has not been "
                    "initialized. Run `tomb init`"
                ) % name
                click.echo(click.style(msg, fg='red', bold=True))
                sys.exit(1)

    click.echo('Done upgrading')


@db.command()
@click.pass_context
def downgrade(ctx):
    """
    Downgrade the database to revision
    """
    try:
        downgrade_path = get_downgrade_path(ctx.obj.db_path)
    except NoMigrationsFoundException:
        click.echo(
            "Did not find any migrations to run in %s" % ctx.obj.db_path
        )
        click.echo(
            "Have you tried running `tomb db revision -m <description>`?"
        )
        sys.exit(1)

    for revision in downgrade_path:
        for name, engine in ctx.obj.db_engines.items():
            click.echo('Running downgrade %s' % revision)
            revision.upgrade(engine)
            engine.update_revision(revision.version - 1)

    click.echo('Done downgrading')


@db.command()
@click.pass_context
def init(ctx):
    """
    Create initial tracking tables for tomb_migrate
    """
    engines = ctx.obj.db_engines
    for key, engine in engines.items():
        click.echo('Initializing %s' % engine)
        try:
            engine.initialize_marker()
        except AlreadyInitializedException:
            click.echo('%s is already initialized' % engine)

    click.echo("done initializing databases")


@db.command()
@click.option(
    '--message', '-m',
    help='Short description about the revision',
    required=True
)
@click.pass_context
def revision(ctx, message):
    """
    Generates a new revision file
    """
    fname = create_new_revision(ctx.obj.db_path, message)

    click.echo('Created new revision file at %s' % fname)
