import click
import os

from tomb_migrate.utils import get_engines_from_settings
from tomb_migrate.utils import get_upgrade_path


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
    upgrade_path = get_upgrade_path(ctx.obj.db_path)
    for revision in upgrade_path:
        click.echo('Running upgrade %s' % revision)
        revision.upgrade(ctx.obj.db_engines)
    click.echo('Done upgrading')


@db.command()
def downgrade():
    """
    Downgrade the database to revision
    """
    click.echo('downgrading database')


@db.command()
def init():
    """
    Create initial tracking tables for tomb_migrate
    """
    click.echo("setting up!")
