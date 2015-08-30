import click


@click.group(context_settings={'help_option_names': ['-h', '--help']})
@click.option(
    '--path', '-p',
    type=click.Path(resolve_path=True),
    default='./db/',
    help='The path the migration files live'
)
@click.pass_context
def db(ctx, path):
    # settings = ctx.obj.pyramid_env['registry'].settings
    # TODO: load up DB Settings
    return None


@db.command()
def upgrade():
    """
    Upgrade the database to revision
    """
    click.echo('upgrading database')


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
