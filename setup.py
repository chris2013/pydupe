from setuptools import setup, find_packages

setup(
    name='pydupe',
    version='1.0.0',
    author = 'C. Sachs',
    author_email = 'python@sachsmail.de',
    description = 'A Package to deal with dupes',
    url = 'https://github.com/chris2013/pydupe',
    project_urls={
        "Bug Tracker": "https://github.com/chris2013/pydupe/issues",
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
        'Click', 'rich', 'more_itertools'
    ],
    entry_points={
        'console_scripts': [
            'pydupe = pydupe.cli:cli',
        ],
    },
)
