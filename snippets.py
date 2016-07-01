import logging
import argparse
import psycopg2

#Set the log output file, and the log level
logging.basicConfig(filename = "snippets.log", level = logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet, hidden):
    """   
    Store a snippet with an associated name.

    Returns the name and the snippet
    """
    
    cursor = connection.cursor()
    command = "insert into snippets values (%s, %s); update snippets set hidden = True where keyword = (%s)"
    try:
        
        with connection, connection.cursor() as cursor:
            cursor.execute(command, (name, snippet, name))
    except psycopg2.IntegrityError as e:
        connection.rollback()
        command = "update snippets set message=%s where keyword=%s"
        with connection, connection,cursor() as cursor:
            cursor.execute(command, (snippet, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet

def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet, return '404: Snippet Not Found'.

    Returns the snippet.
    """
    cursor = connection.cursor()
    command = "select message from snippets where keyword = (%s)"
    with connection, connection.cursor() as cursor:
        cursor.execute(command, (name,))
        x = cursor.fetchone()
    if not x:
        #No snippet was found
        return "404: Snippet Not Found"
    return x[0]
    
def catalog():
    """
    Looks up the possible keywords for snippets
    """
    
    cursor = connection.cursor()
    command = "select keyword from snippets where not hidden order by keyword"
    with connection, connection.cursor() as cursor:
        cursor.execute(command)
        catalog = cursor.fetchall()
    logging.debug("Returned all keywords successfully")
    return catalog
    
def search(string):
    """
    Looks up snippets that contain the provided string in the message
    """
    
    cursor = connection.cursor()
    command ="select keyword, message from snippets where message like \'%" + string + "%\' AND not hidden"
    with connection, connection.cursor() as cursor:
        cursor.execute(command,)
        results = cursor.fetchall()
    logging.debug("Found all snippets that contain your string")
    return results
    
def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
   
    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    put_parser.add_argument("--hidden", help = "Hides snippet", action="store_true")
    
    

    # Subparser for the put command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")
    
    #Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="Retrieves available keywords")
    
    #Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Looks up snippets containing the provided string")
    search_parser.add_argument("string", help="String to find message")
    

    arguments = parser.parse_args()
    
    
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command =="catalog":
        keys = catalog()
        print("Catalog of keywords: {!r}".format(keys))
    elif command =="search":
        results = search(**arguments)
        print("Your search was matched with these snippets: {!r}".format(results))

if __name__ == "__main__":
    main()