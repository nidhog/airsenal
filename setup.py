"""FOOBASE Settings

"""

# Default (Host, Port)
(default_host, default_port) = ('localhost', 10000)
default_storage_file = "data_store/data.dat"
default_backlog = 1
# Logging management
LOG_FILE = "logging.log"
#
default_basic_intersection_file_path = 'data_store/basic_intersections.dat'
default_mapreduce_intersection_file_path = 'data_store/mapreduce_intersections.dat'

init_query_stats = {
    'CREATE': {'total':0, 'OK': 0, 'KO': 0},
    'READ'  : {'total':0, 'OK': 0, 'KO': 0},
    'UPDATE': {'total':0, 'OK': 0, 'KO': 0},
    'DELETE': {'total':0, 'OK': 0, 'KO': 0},
    'ALL'   : {'total':0, 'OK': 0, 'KO': 0}
    }    
    
# Response codes
decode_response = {
    '1111':"Error occured.",
    '0000':"CREATE command successful.",
    '0001':"READ command successful.",
    '0011':"UPDATE command successful.",
    '0010':"DELETE command successful.",
    '0100':"CREATE command unsuccessful.",
    '0101':"READ command unsuccessful.",
    '0111':"UPDATE command unsuccessful.",
    '0110':"DELETE command unsuccessful.",
    '1110':'Nothing happened in the database.'
    }    

