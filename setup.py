
from distutils.core import setup
setup(
    name='memphis-py',
    packages=['memphis'],
    version='0.1.8',
    license='GPL',
    description='A powerful message broker for developers',
    # long_description = file: README.md
    # long_description_content_type = text/markdown; charset=UTF-8; variant=GF
    readme="README.md",
    author='Memphis.dev',
    author_email='team@memphis.dev',
    url='https://github.com/memphisdev/memphis.py',
    download_url='https://github.com/memphisdev/memphis.py/archive/refs/tags/v0.1.8.tar.gz',
    keywords=['message broker', 'devtool', 'streaming', 'data'],
    install_requires=[
        'asyncio',
        'nats-py'
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