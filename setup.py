from distutils.core import setup

setup(
    name='elon',
    version='0.0.1',
    author='Omar Bohsali',
    author_email='me@omarish.com',
    packages=['elon'],
    url="http://github.com/omarish/elon",
    license='MIT',
    description='Lightweight async job queue backed by redis.',
    install_requires=['six>=1.11.0', 'aioredis>=1.0.0', 'redis >= 2.10.5']
)
