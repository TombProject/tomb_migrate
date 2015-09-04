import pytest
import mock
from mock import call


@pytest.mark.unit
@mock.patch('tomb_migrate.utils.SourceFileLoader')
@mock.patch('tomb_migrate.utils.isfile')
@mock.patch('tomb_migrate.utils.isdir')
@mock.patch('tomb_migrate.utils.listdir')
def test_get_upgrade_path(list_dir, isdir, isfile, file_loader):
    from tomb_migrate.utils import get_upgrade_path
    from tomb_migrate.utils import Revision
    isfile.return_value = True
    isdir.return_value = True

    list_dir.return_value = [
        '00006_qix.py',
        '00001_foo.py',
        '00003_baz.py',
        '00002_bar.py',
        '00004_qux.py',
        '00010_peña.py',
        '00011_foo_bar_baz.py',
    ]
    files = get_upgrade_path('boom')
    expected = [
        Revision('00001_foo.py'),
        Revision('00002_bar.py'),
        Revision('00003_baz.py'),
        Revision('00004_qux.py'),
        Revision('00006_qix.py'),
        Revision('00010_peña.py'),
        Revision('00011_foo_bar_baz.py'),
    ]
    assert files == expected


@pytest.mark.unit
@mock.patch('tomb_migrate.utils.SourceFileLoader')
@mock.patch('tomb_migrate.utils.isfile')
@mock.patch('tomb_migrate.utils.isdir')
@mock.patch('tomb_migrate.utils.listdir')
def test_get_upgrade_path_with_version(list_dir, isdir, isfile, file_loader):
    from tomb_migrate.utils import get_upgrade_path
    from tomb_migrate.utils import Revision

    isfile.return_value = True
    isdir.return_value = True

    list_dir.return_value = [
        '00006_qix.py',
        '00001_foo.py',
        '00003_baz.py',
        '00002_bar.py',
        '00004_qux.py',
        '00010_peña.py',
        '00011_foo_bar_baz.py',
    ]

    files = get_upgrade_path('boom', version=3)

    expected = [
        Revision('00003_baz.py'),
        Revision('00004_qux.py'),
        Revision('00006_qix.py'),
        Revision('00010_peña.py'),
        Revision('00011_foo_bar_baz.py'),
    ]

    assert files == expected


@pytest.mark.unit
def test_get_upgrade_path_bad_file():
    from tomb_migrate.utils import get_upgrade_path

    with mock.patch('tomb_migrate.utils.listdir') as l:
        with mock.patch('tomb_migrate.utils.isfile') as i:
            with mock.patch('tomb_migrate.utils.SourceFileLoader'):
                i.return_value = True
                l.return_value = [
                    '00006_qix.py',
                    '00001_foo.py',
                    '00003_baz.py',
                    '00002_bar.py',
                    '00004_qux.py',
                    '00010_peña.py',
                    'foo.tmp~',
                ]
                with pytest.raises(SystemExit):
                    get_upgrade_path('boom')


@pytest.mark.unit
def test_get_engines_from_settings_psyco():
    from tomb_migrate.utils import get_engines_from_settings
    from tomb_migrate.utils import EngineContainer

    engine = mock.Mock()

    with mock.patch('tomb_migrate.utils.psycopg2') as pg2:
        pg2.connect.return_value = engine

        with mock.patch('tomb_migrate.utils.register_default_jsonb') as reg:
            settings = {
                'auth': {
                    'type': 'postgresql',
                    'host': '127.0.0.1',
                    'port': 5432,
                    'database': 'sontek',
                }
            }
            container = EngineContainer('auth', settings['auth'])
            result = get_engines_from_settings(settings)

    expected = call(database='sontek', host='127.0.0.1', port=5432)
    assert pg2.connect.call_args_list[0] == expected
    assert result['auth'].engine == container.engine
    assert result['auth'].settings == container.settings
    assert reg.called
