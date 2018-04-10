"""Launch the webapp in debug mode.

WARNING: only for development purpose!!
"""

from jitenshea.webapp import app
from jitenshea.webapi import api


app.config['DEBUG'] = True
app.config['TEMPLATES_AUTO_RELOAD'] = True
api.init_app(app)
app.run(port=7987, debug=True)
