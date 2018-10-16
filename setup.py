# -*- coding: utf-8 -*-

import setuptools


with open("README.md") as fobj:
    LONG_DESCRIPTION = fobj.read()

INSTALL_REQUIRES = ["pandas", "requests", "psycopg2-binary", "luigi", 'sqlalchemy',
                    'lxml', 'xgboost', 'daiquiri', 'Flask==1.0.2', 'flask-restplus==0.12.1',
                    'sh', 'seaborn', 'scikit-learn', 'tables']


setuptools.setup(
    name='jitenshea',
    version='0.1',
    license='BSD',
    url='https://github.com/garaud/jitenshea',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    extras_require={'dev': ['pytest', 'pytest-sugar', 'ipython', 'ipdb']},

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
