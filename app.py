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
    SELECT DISTINCT f.film_id, f.title,f.release_year, c.name AS category
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

def film_details(film_id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    
    # Film details + actors + categories
    query = """
    SELECT f.film_id, f.title, f.description,
           GROUP_CONCAT(DISTINCT CONCAT(a.first_name, ' ', a.last_name)) AS actors,
           GROUP_CONCAT(DISTINCT c.name) AS categories,
           f.release_year
    FROM film f
    JOIN film_actor fa ON f.film_id = fa.film_id
    JOIN actor a ON fa.actor_id = a.actor_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE f.film_id = %s
    GROUP BY f.film_id
    """
    cursor.execute(query, (film_id,))
    film = cursor.fetchone()

    
    cursor.execute("""
        SELECT inventory_id 
        FROM inventory
        WHERE film_id = %s
        LIMIT 1
    """, (film_id,))
    inventory = cursor.fetchone()
    film['inventory_id'] = inventory['inventory_id'] if inventory else None

    cursor.close()
    db.close()
    return jsonify(film)


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

@app.route('/api/customers', methods=['GET'])
def get_customers():
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search = request.args.get('search', '')

    page_offset = (page - 1) * per_page

    query = """
        select customer_id, first_name, last_name, email, active
        from customer 
        where first_name like %s or last_name like %s or customer_id like %s
        limit %s offset %s
    """

    val = (f"%{search}%", f"%{search}%", f"%{search}%", per_page, page_offset)

    cursor.execute(query,val)

    data = cursor.fetchall()
    cursor.execute("select count(*) as total from customer")

    total = cursor.fetchone()['total']

    return jsonify({"data": data, "total": total})

@app.route('/api/customers', methods=['POST'])
def add_customer():
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    data = request.json

    query = """
        insert into customer (store_id, first_name, last_name, email, address_id, active)
        values(%s, %s, %s, %s, %s)
    """

    values = (
        data['store_id'],
        data['first_name'],
        data['last_name'],
        data['email'],
        data['address_id'], 
    )

    cursor.execute(query,values)

    db.commit()

    return jsonify({"message": "Customer Added to Database."}), 201

@app.route('/api/customers/<int:id>', methods=['PUT'])
def update_customer(id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)
    data = request.json

    query = """
        update customer
        set first_name=%s, last_name=%s, email=%s, active=%s
        where customer_id=%s
    """

    values = (
        data['first_name'],
        data['last_name'],
        data['email'],
        data['active'],
        id
    )
    
    cursor.execute(query, values)
    db.commit()

    return jsonify({"message": "Updated Customer Information."})

@app.route('/api/customer/<int:id>', methods=['DELETE'])
def delete_customer(id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    cursor.execute("delete from customer where customer_id=%s", (id,))
    db.commit()

    return jsonify({"message":"Customer successfully deleted from database."})


@app.route('/api/customers/<int:id>', methods=['GET'])
def get_customer(id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    # Fetch customer
    cursor.execute("SELECT * FROM customer WHERE customer_id = %s", (id,))
    customer = cursor.fetchone()

    # Fetch rentals
    cursor.execute("""
        SELECT r.rental_id, f.title, r.rental_date, r.return_date
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        WHERE r.customer_id=%s
        ORDER BY r.rental_date DESC
    """, (id,))
    rentals = cursor.fetchall()

    cursor.close()
    db.close()

    return jsonify({
        "customer": customer,
        "rentals": rentals
    })

@app.route ('/api/rentals/<int:rental_id>/return', methods=['PUT'])
def return_rental(rental_id):
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    query = "update rental set return_date = now() where rental_id=%s"

    cursor.execute(query,(rental_id,))
    db.commit()

    return jsonify({"message":"Rental has been returned."})


@app.route('/api/rent',methods=['POST'])
def rent_film():
    db = get_connector()
    cursor = db.cursor(dictionary=True)

    data = request.json

    query = """
        insert into rental (rental_date, inventory_id, customer_id, staff_id)
        values (now(), %s, %s, %s) 
    """

    cursor.execute(query, (data['inventory_id'], data['customer_id'], data['staff_id']))
    db.commit()

    return jsonify({"message": "Customer has succesfully rented out film."}), 201


@app.route("/api/film/<int:film_id>")
def get_film_details(film_id):
    return film_details(film_id)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
