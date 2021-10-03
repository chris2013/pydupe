from setuptools import setup, find_packages

setup(
    name='pydupe',
    version='2.0.0',
    packages=find_packages(),
    python_requires='>3.0',
    include_package_data=True,
    install_requires=[
        'Click', 'rich', 'more_itertools'
    ],
    entry_points={
        'console_scripts': [
            'pydupe = pydupe.cli:cli',
        ],
    },
)
