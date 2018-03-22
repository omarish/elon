import re
from distutils.core import setup

meta_file = open("elon/metadata.py").read()
md = dict(re.findall("__([a-z]+)__\s*=\s*'([^']+)'", meta_file))

setup(
    name='elon',
    version=md['version'],
    author=md['author'],
    author_email=md['authoremail'],
    packages=['elon'],
    url="http://github.com/omarish/elon",
    license='MIT',
    description='Lightweight async job queue backed by redis.',
    install_requires=['six>=1.11.0', 'aioredis>=1.0.0', 'redis >= 2.10.5']
)
