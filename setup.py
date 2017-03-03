from distutils.core import setup

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name="minibolt",
    version=__import__('minibolt').__version__,
    url='https://github.com/nakagami/minibolt/',
    classifiers=classifiers,
    keywords=['Neo4j'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='Simple Neo4j driver',
    long_description=open('README.rst').read(),
    license="MIT",
    py_modules=['minibolt'],
)
