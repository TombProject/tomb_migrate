from setuptools import setup, find_packages

setup(
    name='tomb_testapps',
    packages=find_packages(),
    entry_points={
        'paste.app_factory': [
            'main=tomb_testapps:simple',
        ],
    }
)
