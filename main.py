import ssl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def create_connection() -> Cluster:
    """
    This function creates a connection to the CosmosDB database and returns the session object

    args:
        - None
    
    returns:
        - session: Cassandra session object
    """
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


def create_customer(session: Cluster, email: str, name: str, userid: int) -> None:
    """
    This function inserts a new customer into the 'clienteid' table

    args:
        - session: Cassandra session object
        - email: str
        - name: str
        - userid: int
    
    returns:
        - None
    """
    query = """
        INSERT INTO clienteid (email, name, userid) VALUES (%s, %s, %s)
    """
    session.execute(query, (email, name, userid))

    print("[+] Customer inserted successfully")


def show_customers(session) -> None:
    """
    This function retrieves all the customers from the 'clienteid' table

    args:
        - session: Cassandra session object
    
    returns:
        - None
    """
    query = """
        SELECT * FROM clienteid
    """
    rows = session.execute(query)

    for row in rows:
        print(f"{row.userid} - {row.name} - {row.email}")


if __name__ == "__main__":
    session = create_connection()
    # create_customer(session, "miguel@gmail.com", "Miguel", 5)
    show_customers(session)
