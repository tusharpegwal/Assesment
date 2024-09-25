from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import requests

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///jokes.db'
db = SQLAlchemy(app)

class Joke(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(50))
    type = db.Column(db.String(10))
    joke = db.Column(db.String(500))
    setup = db.Column(db.String(500))
    delivery = db.Column(db.String(500))
    nsfw = db.Column(db.Boolean)
    political = db.Column(db.Boolean)
    sexist = db.Column(db.Boolean)
    safe = db.Column(db.Boolean)
    lang = db.Column(db.String(10))

db.create_all()

def fetch_jokes():
    url = "https://v2.jokeapi.dev/joke/Any?amount=100"
    response = requests.get(url)
    jokes = response.json().get('jokes', [])
    
    processed_jokes = []
    for joke in jokes:
        processed_jokes.append(Joke(
            category=joke.get('category'),
            type=joke.get('type'),
            joke=joke.get('joke') if joke.get('type') == 'single' else None,
            setup=joke.get('setup') if joke.get('type') == 'twopart' else None,
            delivery=joke.get('delivery') if joke.get('type') == 'twopart' else None,
            nsfw=joke.get('flags', {}).get('nsfw'),
            political=joke.get('flags', {}).get('political'),
            sexist=joke.get('flags', {}).get('sexist'),
            safe=joke.get('safe'),
            lang=joke.get('lang')
        ))
    return processed_jokes
@app.route('/fetch_jokes', methods=['GET'])
def fetch_and_store_jokes():
    jokes = fetch_jokes()
    for joke in jokes:
        db.session.add(joke)
    db.session.commit()
    return jsonify({"message": "Jokes fetched and stored successfully!"}), 200

if __name__ == '__main__':
    app.run(debug=True)

