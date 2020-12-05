from flask import render_template
from flask import current_app as app

@app.route('/')
def home():
    return render_template(
        'index.html',
        title='World Energy and Greenhouse Emission Dashboard',
        description='Course Project for DATA 1050 - Brown University',
    )

@app.route('/pd')
def project_description():
    return render_template(
        'pd.html',
        title='Project Description'
    )
#
# @app.route('/js')
# def