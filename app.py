import os, json
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from dotenv import load_dotenv
from tabulate import tabulate

# Carrega variáveis do .env
load_dotenv()

PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "dbname": os.getenv("PGDATABASE", "demo_db"),
}

# URL do Redis com valor padrão
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def pg_conn():
    return psycopg2.connect(**PG)


def redis_conn():
    return redis.from_url(REDIS_URL, decode_responses=True)


def ensure():
    # Cria a tabela se não existir
    sql = open("db.sql").read()
    with pg_conn() as c:
        with c.cursor() as cur:
            cur.execute(sql)
        c.commit()


def cache_key(i):
    return f"student:{i}"


def create_student(n, e, c):
    sql = "INSERT INTO students (name,email,course) VALUES (%s,%s,%s) RETURNING *;"
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (n, e, c))
            row = cur.fetchone()
            conn.commit()
    # aqui usamos default=str para converter datetime em string
    r = redis_conn()
    r.set(cache_key(row["id"]), json.dumps(row, default=str))
    return row


def read_student(i):
    r = redis_conn()
    k = cache_key(i)
    cached = r.get(k)
    if cached:
        return json.loads(cached)

    sql = "SELECT * FROM students WHERE id=%s;"
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (i,))
            row = cur.fetchone()

    if row:
        r.set(k, json.dumps(row, default=str))
    return row


def update_student(i, n=None, e=None, c=None):
    fields = []
    vals = []
    if n:
        fields.append("name=%s")
        vals.append(n)
    if e:
        fields.append("email=%s")
        vals.append(e)
    if c:
        fields.append("course=%s")
        vals.append(c)

    if not fields:
        # nada pra atualizar, só retorna o aluno
        return read_student(i)

    vals.append(i)
    sql = f"UPDATE students SET {','.join(fields)} WHERE id=%s RETURNING *;"
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, vals)
            row = cur.fetchone()
            conn.commit()

    r = redis_conn()
    if row:
        r.set(cache_key(i), json.dumps(row, default=str))
    return row


def delete_student(i):
    sql = "DELETE FROM students WHERE id=%s;"
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (i,))
            ok = cur.rowcount > 0
            conn.commit()
    r = redis_conn()
    r.delete(cache_key(i))
    return ok


def list_students():
    sql = "SELECT * FROM students ORDER BY id;"
    with pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()


def main():
    ensure()
    while True:
        print("\n1-Create 2-Read 3-Update 4-Delete 5-List 0-Exit")
        c = input("» ")
        if c == '1':
            n = input("Name: ")
            e = input("Email: ")
            co = input("Course: ")
            print(create_student(n, e, co))
        elif c == '2':
            i = int(input("ID: "))
            print(read_student(i))
        elif c == '3':
            i = int(input("ID: "))
            n = input("Name: ") or None
            e = input("Email: ") or None
            co = input("Course: ") or None
            print(update_student(i, n, e, co))
        elif c == '4':
            i = int(input("ID: "))
            print(delete_student(i))
        elif c == '5':
            print(tabulate(list_students(), headers="keys", tablefmt="grid"))
        elif c == '0':
            break


if __name__ == "__main__":
    main()
