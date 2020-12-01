from flask import render_template
from flask import current_app as app

@app.route('/')
def home():
    return render_template(
        'index.jinja2',
        title='World Energy and Greenhouse Emission Dashboard',
        description='Course Project for DATA 1050 - Brown University',
        body='Thanks Flask'
    )
