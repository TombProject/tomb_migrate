import pytest
import mock

from click.testing import CliRunner


@pytest.mark.unit
def test_db_help():
    from tomb_cli.main import cli
    runner = CliRunner()
    result = runner.invoke(
        cli, ['-c', './tests/fixtures/complete_app.yaml', 'db']
    )

    assert result.exit_code == 0, result.output
    assert '-h, --help' in result.output


@pytest.mark.unit
def test_db_upgrade():
    from tomb_cli.main import cli
    runner = CliRunner()
    engine = mock.Mock()

    with mock.patch('tomb_migrate.utils.psycopg2') as pg2:
        pg2.connect.return_value = engine
        with mock.patch('tomb_migrate.utils.register_default_jsonb'):
            result = runner.invoke(
                cli, [
                    '-c',
                    './tests/fixtures/complete_app.yaml',
                    'db',
                    '-p',
                    './tests/migrations',
                    'upgrade'
                ]
            )

    assert result.exit_code == 0, result.output
    expected = '''\
Running upgrade <Revision: version=1, desc=foo>
upgrade 00001
Running upgrade <Revision: version=2, desc=bar>
upgrade 00002
Done upgrading
'''
    assert expected == result.output
