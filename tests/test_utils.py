import pytest
import mock


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
