import inspect
import importlib
from pickle import loads, dumps

module = importlib.import_module('lokomotiv.sync')
args = [b'jaime', b'gunter']

print(module.__dict__['hello'](*args))