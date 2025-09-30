from flask import Flask, jsonify, request
import mysql.connector
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "johna", 
    "database": "sakila"
}

def get_connector():
    return mysql.connector.connect(**dbconfig)

@app.route("/api/top5-rented-films")
def get_top5_rented_films():
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    query = """
    SELECT f.film_id, f.title, c.name AS category, COUNT(*) AS rental_count
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY f.film_id, f.title, c.name
    ORDER BY rental_count DESC
    LIMIT 5;
    """

    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

@app.route("/api/top5-actors")
def top5_actors():
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    query = """
    SELECT a.actor_id, a.first_name, a.last_name, COUNT(r.rental_id) AS rentals
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY a.actor_id, a.first_name, a.last_name
    ORDER BY rentals DESC
    LIMIT 5;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)


@app.route("/api/films")
def search_films():
    q = request.args.get("q", "")
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    query = """
    SELECT DISTINCT f.film_id, f.title, c.name AS category
    FROM film f
    JOIN film_actor fa ON f.film_id = fa.film_id
    JOIN actor a ON fa.actor_id = a.actor_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE f.title LIKE %s OR a.first_name LIKE %s OR a.last_name LIKE %s OR c.name LIKE %s
    LIMIT 20
    """
    pattern = f"%{q}%"
    cursor.execute(query, (pattern, pattern, pattern, pattern))
    results = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify(results)

def actor_details(actor_id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    
    cursor.execute("SELECT actor_id, first_name, last_name FROM actor WHERE actor_id=%s", (actor_id,))
    actor = cursor.fetchone()
   
    query = """
    SELECT f.film_id, f.title, COUNT(r.rental_id) AS rental_count
    FROM film f
    JOIN film_actor fa ON f.film_id = fa.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE fa.actor_id = %s
    GROUP BY f.film_id, f.title
    ORDER BY rental_count DESC
    LIMIT 5
    """
    cursor.execute(query, (actor_id,))
    top_films = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify({"actor": actor, "top_films": top_films})

@app.route("/api/film/<int:film_id>")
def film_details(film_id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    query = """
    SELECT f.film_id, f.title, f.description, c.name AS category, f.release_year
    FROM film f
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE f.film_id = %s
    """
    cursor.execute(query, (film_id,))
    result = cursor.fetchone()
    cursor.close()
    db.close()
    return jsonify(result)

@app.route("/api/actors/<int:actor_id>")
def get_actor_details(actor_id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    query = """
    SELECT a.actor_id, a.first_name, a.last_name, 
           GROUP_CONCAT(f.title ORDER BY f.title) AS films
    FROM actor a
    LEFT JOIN film_actor fa ON a.actor_id = fa.actor_id
    LEFT JOIN film f ON fa.film_id = f.film_id
    WHERE a.actor_id = %s
    GROUP BY a.actor_id
    """
    cursor.execute(query, (actor_id,))
    actor = cursor.fetchone()

    cursor.close()
    db.close()

    if actor:
        return jsonify(actor)
    else:
        return jsonify({"error": "Actor not found"}), 404


if __name__ == "__main__":
    app.run(debug=True, port=5000)
