from setuptools import setup, find_packages

setup(
    name='tomb_migrate_testapps',
    packages=find_packages(),
    entry_points={
        'paste.app_factory': [
            'main=tomb_migrate_testapps:simple',
        ],
    }
)
