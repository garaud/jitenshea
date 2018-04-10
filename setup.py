# -*- coding: utf-8 -*-

import setuptools


with open("README.md") as fobj:
    LONG_DESCRIPTION = fobj.read()

INSTALL_REQUIRES = ["pandas", "requests", "psycopg2-binary", "luigi", 'sqlalchemy',
                    'lxml', 'xgboost', 'daiquiri', 'flask-restplus', 'sh',
                    'scikit-learn', 'tables']


setuptools.setup(
    name='jitenshea',
    version='0.1',
    license='BSD',
    url='https://github.com/garaud/jitenshea',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,

    author="Damien Garaud",
    author_email='damien.garaud@gmail.com',
    description="Bicycle-sharing data analysis",
    long_description=LONG_DESCRIPTION,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ]
)
