from airflow.hooks.base import BaseHook
import clickhouse_connect


def get_clickhouse_client(conn_id='clickhouse'):
    """Obtém um cliente Clickhouse.

    Esta função cria e retorna um cliente Clickhouse usando as credenciais armazenadas na conexão do Airflow.

    Args:
        conn_id (str, optional): Identificador da conexão do Airflow. Defaults to 'clickhouse'.

    Returns:
        clickhouse_connect.client.Client: Um cliente Clickhouse configurado com as credenciais e parâmetros especificados.
    """
    try:
        hook = BaseHook.get_connection(conn_id)
        host = hook.host
        port = hook.port
        username = hook.login
        password = hook.password
    except:
        host = 'clickhouse'
        port = 8123
        username = 'default'
        password = 'default'
        
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        connect_timeout=20000,
        send_receive_timeout=20000,
    )
    return client
