import re
from setuptools import setup
from os import path

project_path = path.abspath(path.dirname(__file__))


meta_file = open(path.join(project_path, "elon", "metadata.py")).read()
md = dict(re.findall(r"__([a-z]+)__\s*=\s*'([^']+)'", meta_file))

with open(path.join(project_path, 'README.md')) as f:
    long_description = f.read()


setup(
    name='elon',
    version=md['version'],
    author=md['author'],
    author_email=md['authoremail'],
    packages=['elon'],
    url="http://github.com/omarish/elon",
    license='MIT',
    description='Lightweight async job queue backed by redis.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=['six>=1.11.0', 'aioredis>=1.0.0', 'redis>=2.10.5']
)
