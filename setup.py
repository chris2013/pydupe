from setuptools import setup, find_packages

setup(
    name='pydupe',
    version='1.1.1',
    author = 'C. Sachs',
    author_email = 'python@sachsmail.de',
    description = 'A Package to deal with dupes',
    url = 'https://github.com/chsachs/pydupe',
    project_urls={
        "Bug Tracker": "https://github.com/chsachs/pydupe/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],   
    package_dir = {"": "src"},
    packages = find_packages(where="src"),
    python_requires='>3.0',
    include_package_data=True,
    install_requires=[
        'Click>=8.0.3',
        'rich>=10.10.0',
        'more_itertools>=8.12.0',
        'types-setuptools>=57.4.9',
        'attrs',
        'typing'
    ],
    entry_points={
        'console_scripts': [
            'pydupe = pydupe.cli:cli',
        ],
    },
)
