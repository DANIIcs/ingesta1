import boto3
import mysql.connector

def obtener_datos():
    db_config = {
        'user': 'mysqluser',
        'password': 'mysqlpass',
        'host': 'mysql_db',  # Nombre del servicio en docker-compose
        'database': 'inventario_db'
    }
    conexion = mysql.connector.connect(**db_config)
    cursor = conexion.cursor(dictionary=True)
    cursor.execute("SELECT * FROM productos")  # Ajusta la tabla según el esquema
    datos = cursor.fetchall()
    cursor.close()
    conexion.close()
    return datos

def cargar_a_s3(data):
    s3 = boto3.client('s3')
    with open('/tmp/datos.csv', 'w') as f:
        for row in data:
            f.write(','.join(map(str, row.values())) + '\n')
    s3.upload_file('/tmp/datos.csv', 'tu_bucket_s3', 'datos_inventario.csv')

if __name__ == "__main__":
    datos = obtener_datos()
    cargar_a_s3(datos)
