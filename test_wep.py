"""FOOBASE Server

"""
import sys
import json
import time
import socket
import foosettings
import multiprocessing
from mapreduce.mapreduce import MapReduce
import mapreduce.settings as mr_settings

#    =================== Logging Management =====================
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=foosettings.LOG_FILE,
                    filemode='a')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s'))
logging.getLogger('').addHandler(console)
logging.debug("\n\n   =====  ===== STARTED :"+__file__+"   =====  =====")
def clear_log():
    open(foosettings.LOG_FILE, 'w').close()
    
#    ===================   FooBase Server   =====================    
class SERVER_STATES(object):
    """Server States
    """
    class BEGIN(object):
        pass
    class STARTED(object):
        pass
    class CLOSED(object):
        pass

class Queue: 
    """FIFO Queue
    Notes: Non thread safe, instance needs to be locked
    inpired from: http://code.activestate.com/recipes/210459-quick-and-easy-fifo-queue-class/
    """
    def __init__(self):
        self.in_stack = []
        self.out_stack = []
    def push(self, obj):
        self.in_stack.append(obj)
    def pop(self):
        if not self.out_stack:
            while self.in_stack:
                self.out_stack.append(self.in_stack.pop())
        return self.out_stack.pop()
        
class FooBaseServer(object):
    """FooBase Server
    """
    def __init__(self, host = foosettings.default_host, port = foosettings.default_port, storage_file = foosettings.default_storage_file, backlog = foosettings.default_backlog):
        logging.info("FooBase Server is being instanciated...")
        self.host = host
        self.port = port
        self.storage_file = storage_file
        self.backlog = backlog
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.state = SERVER_STATES.BEGIN
        self.query_stats = foosettings.init_query_stats
        self.writing_queue = Queue()
        self.writing_queue_lock = multiprocessing.Lock()
        pass
        
    def __str__(self):
        s = "== FooBaseServer Instance =="
        s+= "\n== Host : "+str(self.host)
        s+= "\n== Port : "+str(self.port)
        s+= "\n== Data is stored in file : "+str(self.storage_file)
        s+= "\n== == == == = == == == == =="
        return s
    
    ## Decoding a message
    def decode_query(self, query_string):
        query_key   = None
        query_value = None
        try:
            query_command, query_key, query_value = query_string.strip().split(' ')
        except:
            try:
                query_command, query_key = query_string.strip().split(' ')
            except:
                query_command = query_string.strip()
        return query_command, query_key, query_value
    def handle_query(self, query):
        response_code = '1110'
        response_value = None
        try:
            if(self.state == SERVER_STATES.STARTED):
                command, key, value = self.decode_query(query)
                command = command.upper()
                logging.info("- Received query (command = "+str(command)+", key = "+str(value)+")...")
                if command == 'CREATE':
                    response_code, response_value = self.handle_create_query(key, value)
                elif command == 'READ':
                    response_code, response_value = self.handle_read_query(key)
                elif command == 'UPDATE':
                    response_code, response_value = self.handle_update_query(key, value)
                elif command == 'DELETE':
                    response_code, response_value = self.handle_delete_query(key)  
                elif command == 'GENERATE_INTERSECTIONS':
                    response_code, response_value = self.handle_intersection_query()
        except Exception, e:
            logging.error("Error handling query : "+ str(e))
            response_code = '1111'
        return response_code, response_value                     
    ## Handling a query    
    # Create key/value pair
    def handle_create_query(self, key, value):
        logging.info("- Processing command CREATE on (key = "+str(key)+", value = "+str(value)+")...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.create_query(key, value)
        except Exception, e:
            logging.error( "- ERROR on CREATE : " + str(e))
            response_code = '1111'
        return response_code, response_value
    # Store key/value pair
    def create_query(self, key, value):
        logging.info("- The following data is being stored in the storage file (key = "+str(key)+", value = "+str(value)+")...")
        response_code, response_value = '1110', (key, value)
        data_store_file = open( (self.storage_file), "r")
        try:
            data_buffer = json.load(data_store_file)
        except ValueError: 
            data_buffer = {}
        data_store_file.close()
        if key in data_buffer.keys():
            logging.warning(" > Key : "+str(key)+" is already present."
                         +"\n To replace its content please use the UPDATE command"
                         +"\n To delete the key please use the DELETE command  TACK :)")
            response_code = '0100'
        else:
            data_buffer[key] = value
            self.writing_queue_lock.acquire()
            self.writing_queue.push(data_buffer)
            self.writing_queue_lock.release()
            logging.info("      > Data was stored successfully :)")
            response_code = '0000'
        return response_code, response_value
        
    # Read value from key 
    def handle_read_query(self, key):
        logging.info("- Processing command READ on (key = "+str(key)+")...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.read_query(key)
            if(response_value is None):
                logging.debug("- Oh oh, returned value is None")
            else:
                logging.debug("- Returned value is : " + str(response_value))
        except Exception, e:
            logging.error( "- ERROR on READ : " + str(e))
            response_code = '1111'
        return response_code, response_value
    # Read value from key using the storage file
    def read_query(self, key):
        logging.info("- The following key is being read from the storage file: key = "+str(key)+"...")
        response_code, response_value = '1110', None
        data_store_file = open( (self.storage_file), "r")
        try:
            data_buffer = json.load(data_store_file)
            if not(key in data_buffer.keys()):
                response_code = '0101'
                logging.warning(" > Key : "+str(key)+" is not present.  TACK :)")
            else:
                response_value = data_buffer[key]
                logging.info("      > Data was read successfully :)")
                response_code = '0001'
        except ValueError: 
            logging.warning(" > Data file seems to be empty! :'(")
            response_code = '0101'
        data_store_file.close()
        return response_code, response_value
        
    # Update key with new value
    def handle_update_query(self, key, value):
        logging.info("- Processing command UPDATE on (key = "+str(key)+", value = "+str(value)+")...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.update_query(key, value)
        except Exception, e:
            logging.error( "- ERROR on UPDATE : " + str(e))
            response_code = '1111'
        return response_code, response_value
    # Update key/value pair in storage file
    def update_query(self, key, value):
        logging.info("- The following data is being stored in the storage file (key = "+str(key)+", value = "+str(value)+")...")
        response_code, response_value = '1110', {'key':key, "new value": None, "old value":None}
        data_store_file = open( (self.storage_file), "r")
        try:
            data_buffer = json.load(data_store_file)
        except ValueError: 
            data_buffer = {}
        data_store_file.close()
        if not(key in data_buffer.keys()):
            logging.warning(" > Key : "+str(key)+" is not present."
                         +"\n To create it please use the CREATE command instead  TACK :)")
            response_code, response_value = '0111' , {'key':key, "new value": value, "old value":None}
        else:
            old_value = data_buffer[key]
            data_buffer[key] = value
            self.writing_queue_lock.acquire()
            self.writing_queue.push(data_buffer)
            self.writing_queue_lock.release()
            logging.info("      > Data was updated successfully :)")
            response_code, response_value = '0011' ,{'key':key, "new value": value, "old value":old_value}
        return response_code, response_value
    # Delete key
    def handle_delete_query(self, key):
        logging.info("- Processing command DELETE on (key = "+str(key)+")...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.delete_query(key)
        except Exception, e:
            logging.error( "- ERROR on DELETE : " + str(e))
            response_code = '1111'
        return response_code, response_value
    # Delete key from storage file
    def delete_query(self, key):
        logging.info("- The following data is being deleted from the storage file (key = "+str(key)+")...")
        response_code, response_value = '1110' , key
        data_store_file = open((self.storage_file), "r")
        try:
            data_buffer = json.load(data_store_file)
            if not(key in data_buffer.keys()):
                logging.warning(" > Key : "+str(key)+" is not present."
                             +"\n Things that do not exist cannot be deleted  TACK :)")
                response_code = '0110'
            else:
                old_value = data_buffer[key]
                del data_buffer[key]
                self.writing_queue_lock.acquire()
                self.writing_queue.push(data_buffer)
                self.writing_queue_lock.release()
                logging.info("      > Data was deleted successfully :)")
                response_code, response_value = '0010', {'key':key, "old value":old_value}
        except ValueError: 
            data_buffer = {}
            response_code = '0110'
        data_store_file.close()
        return response_code, response_value
        
    ## Handle client
    def handle_client(self, client_con, client_adr):
        client_qry = client_con.recv(4096).decode()
        response_code, response_value = self.handle_query(client_qry)
        client_con.sendall('-Response Code: {}\n-Response Message: {}\n-Response Value: {}\n'
                            .format(response_code, foosettings.decode_response[response_code], response_value))
        client_con.close()   
        self.persist_data()
        pass
        
    ## Write data in the queue
    def persist_data(self):
        try:
            while not(self.writing_queue.in_stack == [] and self.writing_queue.out_stack == []):
                next = self.writing_queue.pop()
                if not(next is None):
                    logging.debug(">> Writing data to storage file. Data :"+str(next))
                    data_store_file = open((self.storage_file), "w+")
                    json.dump(next, data_store_file)
                    data_store_file.close()
        except:
            pass
    ## Handle intersection query
    def handle_intersection_query(self):
        pass
    ## Launching the server
    def start(self):
        if self.state == SERVER_STATES.BEGIN:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(self.backlog)
            self.state = SERVER_STATES.STARTED
            while self.state == SERVER_STATES.STARTED:
                client_con, client_adr = self.server_socket.accept()
                logging.info('New connection from [{}]'.format(client_adr))
                client_process = multiprocessing.Process(target=self.handle_client, args=(client_con, client_adr))
                client_process.deamon = True
                client_process.start()
                logging.info("> Started new process for the client. Process: "+str(client_process))
                client_con.close()
                
        else:
            logging.error("Tried to start FooBase Server without being in state BEGIN. Current state: " + str(self.state))
    
#    ==========   FooBase Server Basic Intersections   ==========  
class FooBaseServerBasic(FooBaseServer):
    """FooBaseServerBasic class for handling intersections using a basic sequential algorithm"""
    def __init__(self, host = foosettings.default_host, port = foosettings.default_port, storage_file = foosettings.default_storage_file, backlog = foosettings.default_backlog, basic_intersection_file_path = foosettings.default_basic_intersection_file_path):
        FooBaseServer.__init__(self, host, port, storage_file, backlog)
        self.basic_intersection_file_path = basic_intersection_file_path

    def __str__(self):
        s = "== FooBaseServerBasic Instance =="
        s+= "\n== Host : "+str(self.host)
        s+= "\n== Port : "+str(self.port)
        s+= "\n== Data is stored in file : "+str(self.storage_file)
        s+= "\n== == == == ==  == == == == == =="
        return s
    
    def handle_intersection_query(self):
        logging.info("- Processing command GENERATE_INTERSECTIONS...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.intersection_query()
        except Exception, e:
            logging.error( "- ERROR on GENERATE_INTERSECTIONS : " + str(e))
            response_code = '1111'
        return response_code, response_value
    def intersection_query(self):
        logging.info("- Handling intersection query, basic solution...")
        response_code, response_value = '1110' , None
        data_store_file = open((self.storage_file), "r")
        try:
            intersections = {}
            data_buffer = json.load(data_store_file)
            time_mil = (time.time() * 1000)
            sorted_data_buffer_indexes = sorted(data_buffer)
            for i in range(len(sorted_data_buffer_indexes)):
                for j in xrange(i+1,len(sorted_data_buffer_indexes)):
                    key = sorted_data_buffer_indexes[i]
                    second_key = sorted_data_buffer_indexes[j]
                    if not (second_key == key):
                        intersections[str(key)+' '+str(second_key)] = ''.join(set(data_buffer[key].split(',')).intersection(set(data_buffer[second_key].split(','))))
            time_mil = (time.time() * 1000) - time_mil
            logging.info(" # # TIME IN Milliseconds: "+str(time_mil))
            intersection_file = open(self.basic_intersection_file_path, 'w+')
            json.dump(intersections, intersection_file)
            intersection_file.close()
            if len(intersections)>10:
                response_value = {intersections.keys()[i]:intersections[intersections.keys()[i]]  for i in range(10) if  len(intersections[intersections.keys()[i]])>0}
            else:
                response_value = {k:v for (k,v) in intersections.items() if len(v)>0}
        except ValueError, e: 
            data_buffer = {}
            response_code = '1111'
        data_store_file.close()
        return response_code, response_value
        
#    =======   FooBase Server MapReduce Intersections   =========
class CommonFriends(MapReduce):
    """CommonFriends example implementation
    """
    def __init__(self, input_dir, output_dir, n_mappers=1, n_reducers=1):
        MapReduce.__init__(self,  input_dir, output_dir, n_mappers, n_reducers)

    def mapper(self, key, value):
        """Map function for the common friends example
        """
        results = []
        default_count = 1
        i = 0
        n = len(value)
        # seperate line into words
        for line in value.split('\n'):
            if not(line is None or len(line)<1):
                node, rest =  line.split('#')
                node = int(node.strip())
                rest = map(int, rest.strip().split(' '))
                for n_node in rest:
                    if(node<n_node):
                        pair = str(n_node)+" "+str(node)
                    else:
                        pair = str(node)+" "+str(n_node)
                    results.append([pair, rest])
        return results

    
    def reducer(self, key, values):
        """Reduce function implementation for the commomn friends example
        """
        d = [set(value) for value in values]
        if(len(d)>1):
            intersection = set.intersection(*d)
            return str(key)+" # "+' '.join([str(x) for x in (list(intersection))])
        return str(key)+" # "    
class FooBaseServerMapReduce(FooBaseServer):
    """FooBaseServerBasic class for handling intersections using a basic sequential algorithm
    Uses 4 mappers and 4 reducers"""
    def __init__(self, host = foosettings.default_host, port = foosettings.default_port, storage_file = foosettings.default_storage_file, backlog = foosettings.default_backlog, mapreduce_intersection_file_path = foosettings.default_mapreduce_intersection_file_path):
        FooBaseServer.__init__(self, host, port, storage_file, backlog)
        self.mapreduce_intersection_file_path = mapreduce_intersection_file_path

    def __str__(self):
        s = "== FooBaseServerMapReduce Instance =="
        s+= "\n== Host : "+str(self.host)
        s+= "\n== Port : "+str(self.port)
        s+= "\n== Data is stored in file : "+str(self.storage_file)
        s+= "\n== == == == ==  == == == == == =="
        return s
    def handle_intersection_query(self):
        logging.info("- Processing command GENERATE_INTERSECTIONS...")
        response_code = '1110'
        response_value = None
        try:
            response_code, response_value = self.intersection_query()
        except Exception, e:
            logging.error("- ERROR on GENERATE_INTERSECTIONS : " + str(e))
            response_code = '1111'
        return response_code, response_value
    def intersection_query(self):
        logging.info("- Handling intersection query, MapReduce solution...")
        response_code, response_value = '1110' , None
        data_store_file = open((self.storage_file), "r")
        try:
            intersections = {}
            data_buffer = json.load(data_store_file)
            mr_input_file = open((mr_settings.get_input_file()), "w+")
            for key in data_buffer:
                line = str(key)+" # "+' '.join((data_buffer[key].split(',')))+'\n'
                mr_input_file.write(line)
            mr_input_file.close()
            common_friends = CommonFriends('mapreduce/input_files', 'mapreduce/output_files')
            time_mil = int(round(time.time() * 1000))
            common_friends.run()
            time_mil = int(round(time.time() * 1000)) - time_mil
            logging.info(" # # TIME IN Milliseconds: "+str(time_mil))
            result =  [line for line in common_friends.join_outputs()]
            for line in result:
                key, values = line.split('#')
                key = key.strip()
                values= ''.join(values.strip().split(" "))
                intersections[key] = values
            intersection_file = open(self.mapreduce_intersection_file_path, 'w+')
            json.dump(intersections, intersection_file)
            intersection_file.close()
            if len(intersections)>10:
                response_value = {intersections.keys()[i]:intersections[intersections.keys()[i]]  for i in range(10) if  len(intersections[intersections.keys()[i]])>0}
            else:
                response_value = {k:v for (k,v) in intersections.items() if len(v)>0}
        except ValueError: 
            data_buffer = {}
            response_code = '1111'
        data_store_file.close()
        return response_code, response_value

    
            
def main():
    host, port = foosettings.default_host, foosettings.default_port
    if (len(sys.argv) >= 3):
        host, port = sys.argv[1], int(sys.argv[2])
    clear_log()
    fbserv = FooBaseServer(host, port)
    print fbserv
    fbserv.start()
    
if __name__=="__main__":
    main()            
        
            
        
     
