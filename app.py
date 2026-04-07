import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'core'))
exec(open(os.path.join(os.path.dirname(__file__), 'core', 'app.py')).read())
