
from distutils.core import setup
setup(
    name='memphis-py',
    packages=['memphis-py'],
    version='0.1.0',
    license='GPL',
    description='A powerful message broker for developers',
    author='Memphis.dev',
    author_email='team@memphis.dev',
    url='https://github.com/memphisdev/memphis.py',
    download_url='https://github.com/memphisdev/memphis.py/archive/refs/tags/v0.1.0.tar.gz',
    keywords=['message broke', 'devtool', 'streaming', 'data'],
    install_requires=[
        'nats-py',
        'pymitter',
        'asyncio',
        'requests'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Dev Tools',
        'License :: GNU License',
        'Programming Language :: Python :: +3.7',
    ],
)
