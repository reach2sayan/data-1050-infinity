from app_files import init_app

app = init_app()

if __name__ == '__main__':
#    app.run(debug=True)
     app.run(host='0.0.0.0', debug=True)
