import os
import json
import functions_framework
import pymysql
from datetime import datetime

DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]

def get_connection():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        print("Error conectando a MySQL:", e)
        raise

def datetime_handler(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Tipo no serializable")

@functions_framework.http
def api_handler(request):
    metodo = request.method
    path = request.path 

    try:
        conn = get_connection()
        cursor = conn.cursor()

        if path == "/students" and metodo == "POST":
            data = request.get_json()
            nombre = data.get("nombre")
            correo = data.get("correo")

            if not nombre or not correo:
                return (
                    json.dumps({"error": "Faltan campos obligatorios: nombre y correo"}, ensure_ascii=False),
                    400,
                    {"Content-Type": "application/json"}
                )

            cursor.execute(
                "INSERT INTO students (nombre, correo, fecha_registro) VALUES (%s, %s, NOW())",
                (nombre, correo)
            )
            conn.commit()
            return (
                json.dumps({"mensaje": "Estudiante creado correctamente"}, ensure_ascii=False),
                201,
                {"Content-Type": "application/json"}
            )

        elif path == "/courses" and metodo == "POST":
            data = request.get_json()
            titulo = data.get("titulo")
            descripcion = data.get("descripcion", "")

            if not titulo:
                return (
                    json.dumps({"error": "Falta campo obligatorio: titulo"}, ensure_ascii=False),
                    400,
                    {"Content-Type": "application/json"}
                )

            cursor.execute(
                "INSERT INTO courses (titulo, descripcion) VALUES (%s, %s)",
                (titulo, descripcion)
            )
            conn.commit()
            return (
                json.dumps({"mensaje": "Curso creado correctamente"}, ensure_ascii=False),
                201,
                {"Content-Type": "application/json"}
            )

        elif path == "/enrollments" and metodo == "POST":
            data = request.get_json()
            student_id = data.get("student_id")
            course_id = data.get("course_id")

            if not student_id or not course_id:
                return (
                    json.dumps({"error": "Faltan campos obligatorios: student_id y course_id"}, ensure_ascii=False),
                    400,
                    {"Content-Type": "application/json"}
                )

            cursor.execute(
                "INSERT INTO enrollments (student_id, course_id, puntaje, estado, fecha_matricula) VALUES (%s, %s, %s, %s, NOW())",
                (student_id, course_id, 100, "Activo")
            )
            conn.commit()
            return (
                json.dumps({"mensaje": "Matrícula creada correctamente"}, ensure_ascii=False),
                201,
                {"Content-Type": "application/json"}
            )

        elif path.startswith("/enrollments/") and metodo == "PUT":
            enrollment_id = path.split("/")[-1]
            data = request.get_json()
            estado = data.get("estado")
            puntaje = data.get("puntaje")

            if not estado and puntaje is None:
                return (
                    json.dumps({"error": "Falta al menos un campo para actualizar"}, ensure_ascii=False),
                    400,
                    {"Content-Type": "application/json"}
                )

            if estado and puntaje is not None:
                cursor.execute(
                    "UPDATE enrollments SET estado=%s, puntaje=%s WHERE id=%s",
                    (estado, puntaje, enrollment_id)
                )
            elif estado:
                cursor.execute(
                    "UPDATE enrollments SET estado=%s WHERE id=%s",
                    (estado, enrollment_id)
                )
            elif puntaje is not None:
                cursor.execute(
                    "UPDATE enrollments SET puntaje=%s WHERE id=%s",
                    (puntaje, enrollment_id)
                )

            conn.commit()
            return (
                json.dumps({"mensaje": "Matrícula actualizada correctamente"}, ensure_ascii=False),
                200,
                {"Content-Type": "application/json"}
            )

        elif path.startswith("/students") and path.endswith("/enrollments") and metodo == "GET":
            parts = path.split("/")
            student_id = parts[2] if len(parts) > 2 and parts[2].isdigit() else None

            if student_id:
                cursor.execute(
                    """SELECT e.id AS enrollment_id, s.nombre AS student_name, c.titulo AS course_name,
                              e.puntaje, e.estado, e.fecha_matricula
                       FROM enrollments e
                       JOIN students s ON e.student_id = s.id
                       JOIN courses c ON e.course_id = c.id
                       WHERE e.student_id = %s""",
                    (student_id,)
                )
            else:
                cursor.execute(
                    """SELECT e.id AS enrollment_id, s.nombre AS student_name, c.titulo AS course_name,
                              e.puntaje, e.estado, e.fecha_matricula
                       FROM enrollments e
                       JOIN students s ON e.student_id = s.id
                       JOIN courses c ON e.course_id = c.id"""
                )

            rows = cursor.fetchall()
            return (
                json.dumps(rows, ensure_ascii=False, default=datetime_handler),
                200,
                {"Content-Type": "application/json"}
            )

        else:
            return ("Ruta o método no permitido", 405)

    except Exception as e:
        return (
            json.dumps({"error": str(e)}, ensure_ascii=False),
            500,
            {"Content-Type": "application/json"}
        )
    finally:
        cursor.close()
        conn.close()
