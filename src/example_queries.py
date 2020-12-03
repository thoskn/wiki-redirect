import mysql.connector


def get_connection(host: str, user: str, password: str):
    return mysql.connector.connect(host=host, user=user, password=password)


def total_root_pages_current():
    """
    A function that returns the number of root pages from the current redirect state.
    """
    connection = get_connection("localhost", "root", "pword")
    cursor = connection.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM persistent.redirect WHERE effective_to IS NULL;"
    )
    return next(cursor)[0]


def total_redirect_page_latest_load():
    """
    A function that returns the number of redirect pages from the latest dump.
    """
    connection = get_connection("localhost", "root", "pword")
    cursor = connection.cursor()
    cursor.execute(
        "SELECT batch_timestamp FROM persistent.redirect ORDER BY batch_timestamp DESC LIMIT 1;"
    )
    latest_timestamp = next(cursor)[0]
    cursor.execute(
        f"SELECT COUNT(*) FROM persistent.redirect "
        f"WHERE batch_timestamp='{latest_timestamp}' AND effective_to IS NULL"
    )
    return next(cursor)[0]
