import pytest
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
    result = runner.invoke(
        cli, ['-c', './tests/fixtures/complete_app.yaml', 'db', 'upgrade']
    )

    assert result.exit_code == 0, result.output
    assert 'upgrading database\n' == result.output
