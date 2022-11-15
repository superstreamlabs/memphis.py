
from setuptools import setup
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='memphis-py',
    packages=['memphis'],
    version='0.2.2',
    license='Apache-2.0',
    description='A powerful messaging platform for modern developers',
    long_description=long_description,
    long_description_content_type='text/markdown',
    readme="README.md",
    author='Memphis.dev',
    author_email='team@memphis.dev',
    url='https://github.com/memphisdev/memphis.py',
    download_url='https://github.com/memphisdev/memphis.py/archive/refs/tags/v0.2.2.tar.gz',
    keywords=['message broker', 'devtool', 'streaming', 'data'],
    install_requires=[
        'asyncio',
        'nats-py',
        'protobuf'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
