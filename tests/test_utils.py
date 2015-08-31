import pytest
import mock
from mock import call


@pytest.mark.unit
def test_get_upgrade_path():
    from tomb_migrate.utils import get_upgrade_path
    from tomb_migrate.utils import Revision

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
def test_get_upgrade_path_with_version():
    from tomb_migrate.utils import get_upgrade_path
    from tomb_migrate.utils import Revision

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
                with pytest.raises(Exception):
                    get_upgrade_path('boom')


@pytest.mark.unit
def test_get_engines_from_settings_psyco():
    from tomb_migrate.utils import get_engines_from_settings
    engine = mock.Mock()

    with mock.patch('tomb_migrate.utils.psycopg2') as pg2:
        pg2.connect.return_value = engine

        with mock.patch('tomb_migrate.utils.register_default_jsonb') as reg:
            result = get_engines_from_settings({
                'auth': {
                    'type': 'postgresql',
                    'host': '127.0.0.1',
                    'port': 5432,
                    'database': 'sontek',
                }
            })
    expected = call(database='sontek', host='127.0.0.1', port=5432)
    assert pg2.connect.call_args_list[0] == expected
    assert result == {'auth': engine}
    assert reg.called
