import ssl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


class Customer(BaseModel):
    email: str
    name: str
    userid: int


app = FastAPI()


def create_connection() -> Cluster:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False  
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    endpoint = "urielcluster.cassandra.cosmos.azure.com"
    port = 10350
    username = "urielcluster"
    password = "ptiG4LigB7LbpYjxjQY3yGqivHRbsk5SAeJ1gOrxVzkHp44crJJmfBC2SL7HS2QL9mSeocToZlS0ACDbXNNycQ=="

    auth_provider = PlainTextAuthProvider(username=username, password=password)

    cluster = Cluster([endpoint], port=port, auth_provider=auth_provider, ssl_context=ssl_context)
    session = cluster.connect("clientes")

    print("[+] Connection successful")
    return session

session = create_connection()

@app.post("/create_customer/")
def create_customer(customer: Customer):
    """
    Endpoint para insertar un nuevo cliente en la base de datos.
    Espera recibir un objeto JSON con 'email', 'name' y 'userid'.
    """
    try:
        query = "INSERT INTO clienteid (email, name, userid) VALUES (%s, %s, %s)"
        session.execute(query, (customer.email, customer.name, customer.userid))
        return {"message": "Customer inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/show_customers/")
def show_customers():
    """
    Endpoint para recuperar todos los clientes.
    """
    try:
        query = "SELECT * FROM clienteid"
        rows = session.execute(query)
        return {"customers": [{"userid": row.userid, "name": row.name, "email": row.email} for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
