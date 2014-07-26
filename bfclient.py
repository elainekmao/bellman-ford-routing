import sys, socket, collections, time, json, os, copy
from threading import * 
from select import select

#Sets up UDP socket
def setup(address, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((address, port))
        print "Socket created on {0}:{1}\n".format(address, port)
    except socket.error as msg:
        print "An error has occurred in creating the socket."
        sys.exit()
    return sock

#Helper function to return direct neighbors of client
def get_neighbors():
    neighbors_dict = {}
    for node in nodes:                              #For each node in the client's nodes dictionary, checks if node is a neighbor
        if nodes[node]['is_neighbor']:
            neighbors_dict[node] = nodes[node]      #If so, adds to dictionary
    return neighbors_dict                           #Returns dictionary of neighbors

#Bellman Ford algorithm to calculate the least-cost path from client to all nodes
def Bellman_Ford():
    changed = False                                 #Tracks whether link costs have been updated
    for y in nodes:                                 #For all nodes except for the client node
        if y != self:
            previous_cost = float(nodes[y]['minimum_cost'])     #Saves previous cost to check against
            cost = float("inf")                     #Sets cost to y to be infinity
            nexthop = nodes[y]['nexthop']           #Sets nexthop to be the current nexthop on the route to y
            for v in get_neighbors():               #For all v where v is a neighbor to the client
                if y in nodes[v]['neighbor_costs']:     #If there is a route to y from v    
                    temp_cost = float(nodes[v]['direct_cost']) + float(nodes[v]['neighbor_costs'][y])   #Calculate c(client, v) + c(v,y)
                    if temp_cost < cost:            #If this temporary cost is less than our known minimum cost
                        cost = temp_cost            #Change minimum cost and nexthop values
                        nexthop = v
            if cost != previous_cost:           #Checks if cost has changed from saved previous cost
                changed = True                  #Mark as true so we know that link costs have changed
            nodes[y]['minimum_cost'] = cost         #Update nodes dictionary with new information
            nodes[y]['nexthop'] = nexthop
    if changed:                                     #If link costs have changed, broadcast update to neighbors
        transmit_costs()

#Sends distance vector information to neighbors, implements Poison Reverse algorithm
def transmit_costs():
    costs = {}                                      
    for node in nodes:                                  #For every node the client is aware of
        costs[node] = nodes[node]['minimum_cost']       #Set its cost to that node to the current known minimum cost
    for neighbor in get_neighbors():                    #For every neighbor of the client
        poison_reverse_costs = copy.deepcopy(costs)     #Create a copy of the costs dictionary 
        for destination in costs:                       #For every destination in the costs dictionary
            if (destination != self and destination != neighbor):       #If the destination is neither self nor a direct neighbor
                if nodes[destination]['nexthop'] == neighbor:           #If the nexthop along the route is the neighbor
                    poison_reverse_costs[destination] = float("inf")    #Change the cost from client to destination to infinity
        update = {'costs_update': poison_reverse_costs, 'direct_cost_to_neighbor': nodes[neighbor]['direct_cost']}
        message = json.dumps({'command': 'ROUTEUPDATE', 'update': update})      #Sends update message to neighbor
        neighbor_tuple = tuple([neighbor.split(':')[0], int(neighbor.split(':')[1])])
        sock.sendto(message, neighbor_tuple)
    global send_timer
    send_timer.cancel()                                     #Resets timer
    send_timer = Timer(float(timeout), transmit_costs)
    send_timer.start()

#Updates a node's costs dictionary upon receiving a ROUTEUPDATE message from a neighbor
def update_costs(address, port, **kwargs):  
    neighbor_costs = kwargs['costs_update']         #Dictionary of neighbor's costs
    sender = '{0}:{1}'.format(address, port)
    for neighbor in neighbor_costs:                 #For each node in neighbor's costs
        if neighbor not in nodes:                   #If client was not aware of node
            nodes[neighbor] = default_node()        #Adds node to nodes dictionary
            nodes[neighbor]['minimum_cost'] = neighbor_costs[neighbor] + nodes[sender]['direct_cost']   #Sets minimum cost to be the cost to the neighbor plus the cost from neighbor to that node
            nodes[neighbor]['nexthop'] = sender     #Sets nexthop to be neighbor that sent the update
    if not nodes[sender]['is_neighbor']:            #If the sender is not currently a neighbor (e.g. if link timed out)
        nodes[sender]['minimum_cost'] = kwargs['direct_cost_to_neighbor']   #Makes sender a neighbor
        nodes[sender]['is_neighbor'] = True
        nodes[sender]['direct_cost'] = kwargs['direct_cost_to_neighbor']
    nodes[sender]['neighbor_costs'] = neighbor_costs    #Updates neighbor costs dictionary
    nodes[sender]['last_update'] = time.time()          #Updates time of last update from this node
    Bellman_Ford()                                      #Runs Bellman Ford algorithm

#Takes down a user-specified link
def LINKDOWN(address, port, **kwargs):
    key = '{0}:{1}'.format(address, port)
    node = nodes[key]
    if not node['is_neighbor']:                     #If not currently linked to that node, returns error
        print "You are not linked to that node."
        return
    else:                                           #Otherwise
        node['direct_cost'] = float("inf")          #Sets cost to node to infinity
        node['is_neighbor'] = False                 #Destroys direct link to node
        node['destroyed'] = True                    #Marks link as destroyed
        Bellman_Ford()                              #Runs Bellman Ford algorithm

#Brings up a user-specified link with a user-specified weight
def LINKUP(address, port, **kwargs):
    key = '{0}:{1}'.format(address, port)
    node = nodes[key]
    if not node['destroyed']:                       #If the node was not previously destroyed with LINKDOWN, returns error
        print "You were not previously connected to that node, so you cannot reconnect."
        return
    else:                                           #Otherwise
        node['minimum_cost'] = kwargs['link_cost']  #Sets cost to be user-specified weight
        node['direct_cost'] = kwargs['link_cost']   
        node['is_neighbor'] = True                  #Marks link as neighbor
        node['destroyed'] = False                   #Marks link as not destroyed
        Bellman_Ford()

#Transfers a file to specified node
def TRANSFER(address, port, **kwargs):
    pass                                            #Function implemented below in main body of code

#Shows client's routing table
def SHOWRT():
    print '<' + str(current_time()) + '> Distance vector list is:\n'
    for node in nodes:
        if (node != self and nodes[node]['minimum_cost'] != float("inf")):                            #For all nodes other than self and unreachable nodes, prints routing information
            print 'Destination = ' + node + ', Cost = ' + str(nodes[node]['minimum_cost']) + ', Link = (' + nodes[node]['nexthop'] + ')\n'

#Closes client
def CLOSE():
    print "Socket closed."
    os._exit(1)

#Helper function to display time to user in SHOWRT and TRANSFER functions
def current_time():
    return time.strftime("%b %d %Y, %I:%M:%S %p")

#Automatically populates a new node with default entries
def default_node():
    return {'minimum_cost': float("inf"), 'direct_cost': float("inf"), 'neighbor_costs': collections.defaultdict(lambda: float("inf")), 'is_neighbor': False, 'nexthop': '', 'destroyed': False, 'last_update': float("inf")}

#Creates new node with specified properties
def new_node(address, minimum_cost, is_neighbor, timeout, direct_cost=None, neighbor_costs=None):
    node = default_node()
    node['minimum_cost'] = minimum_cost
    if direct_cost != None:
        node['direct_cost'] = direct_cost
    if neighbor_costs  != None: 
        node['neighbor_costs'] = neighbor_costs
    node['is_neighbor'] = is_neighbor
    if is_neighbor:
        node['nexthop'] = address
        node['last_update'] = time.time()
    return node

#Function gets called when a node "times out"
def timed_out_node(address):
    node = nodes[address]
    node['minimum_cost'] = float("inf")             #Sets minimum cost to infinity
    node['direct_cost'] = float("inf")              #Sets direct cost to infinity
    node['neighbor_costs'][self] = float("inf")     #Sets cost from node to client to infinity
    transmit_costs()                                #Updates neighbors about change
    Bellman_Ford()                                  #Runs Bellman Ford algorithm
    node['is_neighbor'] = False                     #Removes node as a neighbor

#Helper function to read in configuration file
def parse_config(config_file):
    f = open(config_file, 'r')
    i = 0
    file_chunk_to_transfer = ''
    file_sequence_number = ''
    neighbors = collections.defaultdict(lambda: float("inf"))
    for line in f:
        l = line.split()
        if i == 0:
            localport = l[0]
            timeout = l[1]
            if len(l) == 4:
                file_chunk_to_transfer = l[2]
                file_sequence_number = l[3]
            i += 1
        else:
            destination = l[0]
            ipaddress = l[0].split(':')[0]
            port = l[0].split(':')[1]
            weight = l[1]
            neighbors[destination] = weight
    return localport, timeout, neighbors, file_chunk_to_transfer, file_sequence_number

#Helper function to parse user input
def parse_user_input(user_input):
    user_input = user_input.split()
    parsed = {'address': (), 'weight': {}}
    if not user_input:
        print "Please enter a command."
    else:
        command = user_input[0].upper()
        if command not in ['LINKDOWN', 'LINKUP', 'SHOWRT', 'CLOSE', 'TRANSFER']:
            print "That is not a valid command."
            parsed = {}
        else:
            parsed['command'] = command
            if command in ['LINKDOWN', 'LINKUP', 'TRANSFER']:
                args = user_input[1:]
                if command == 'LINKDOWN':
                    if len(args) != 2:
                        print "LINKDOWN requires IP address and port number as arguments."
                        parsed = {}
                    else:
                        ipaddress = user_input[1]
                        port = int(user_input[2])
                        parsed['address'] = (ipaddress, port)
                elif command == 'LINKUP': 
                    if len(args) != 3:
                        print "LINKUP requires IP address, port number and weight as arguments."
                        parsed = {}
                    else:
                        ipaddress = user_input[1]
                        port  = int(user_input[2])
                        parsed['address'] = (ipaddress, port)
                        parsed['weight'] = {'link_cost' :float(user_input[3])}
                elif command == 'TRANSFER':
                    if len(args) != 2:
                        print "TRANSFER requires IP address and port number as arguments."
                        parsed = {}
                    else:
                        ipaddress = user_input[1]
                        port  = int(user_input[2])
                        parsed['address'] = (ipaddress, port)
    return parsed

#Mapping of user input to functions
functionlist = {
    'LINKDOWN' : LINKDOWN,
    'LINKUP' : LINKUP, 
    'TRANSFER' : TRANSFER, 
    'SHOWRT' : SHOWRT,
    'CLOSE' : CLOSE
}

#Main code of program
if __name__ == "__main__": 
    localhost = socket.gethostbyname(socket.gethostname())
    localport, timeout, neighbors, file_chunk_to_transfer, file_sequence_number = parse_config(sys.argv[1])
    sock = setup(localhost, int(localport))                 #Creates socket
    BUFFER = 4096                                           #Sets buffer value
    inputs = [sock, sys.stdin]
    nodes = collections.defaultdict(lambda: default_node()) #Creates nodes dictionary for client
    for neighbor in neighbors:                              #For each neighbor in config file, creates node
        nodes[neighbor] = new_node(address=neighbor, minimum_cost=neighbors[neighbor], is_neighbor=True, timeout=timeout,direct_cost=neighbors[neighbor])
    self = sock.getsockname()                               #For self, creates node
    self = '{0}:{1}'.format(self[0], self[1])
    nodes[self] = new_node(address=self, minimum_cost=0.0, is_neighbor=False, timeout=float("inf"), direct_cost=0.0)
    file_dict = {}                                          #File dictionary to keep track of which file chunks received
    send_timer = Timer(float(timeout), transmit_costs)      #Starts timer
    send_timer.start()
    transmit_costs()                                        #Transmits costs to neighbors
    while True:
        for node in nodes:                                  #Checks to see if any nodes have timed out (loops)
            if time.time() - nodes[node]['last_update'] >= 3*float(timeout):
                timed_out_node(node)
        input_ready, output_ready, except_ready = select(inputs,[],[]) 
        for s in input_ready:
            if s == sys.stdin:                              #If there is user input, do the following
                user_input = sys.stdin.readline()
                parsed = parse_user_input(user_input)
                if not parsed:                              #If input invalid, continue
                    continue
                else:                                       #Otherwise
                    command = parsed['command']             #Determine what the command is
                    if command in ['LINKDOWN', 'LINKUP']:   #If command is LINKDOWN or LINKUP
                        message = json.dumps({'command': command, 'optional': parsed['weight']})    #Inform target node of action
                        sock.sendto(message, parsed['address'])
                    elif command == 'TRANSFER':             #If command is TRANSFER
                        if not (file_chunk_to_transfer and file_sequence_number):       #Check if client has a file to transfer
                            print "You do not have a file to transfer."
                        else:                                                                               #If client has a file to transfer
                            destination = parsed['address']                             
                            destination_key = '{0}:{1}'.format(parsed['address'][0], parsed['address'][1])
                            nexthop = nodes[destination_key]['nexthop']                                     #Determine next hop along route to destination
                            nexthop_tuple = tuple([nexthop.split(':')[0], int(nexthop.split(':')[1])])
                            if nexthop_tuple == destination:                                                #If next hop is destination, set is_destination=True
                                message = json.dumps({'command': command, 'file_chunk_to_transfer': file_chunk_to_transfer, 'sequence_number': file_sequence_number, 'destination': destination, 'is_destination': True, 'path': [nexthop]})
                            else:                                                                           #Otherwise, set is_destination=False
                                message = json.dumps({'command': command, 'file_chunk_to_transfer': file_chunk_to_transfer, 'sequence_number': file_sequence_number, 'destination': destination, 'is_destination': False, 'path': [nexthop]})
                            sock.sendto(message, nexthop_tuple)                         #Send message to the next hop node
                            f = open(file_chunk_to_transfer, 'rU')                      #Open file to transfer
                            data = f.read(BUFFER)                                       #Read file into buffer
                            while (data):                                               #Transfer to next hop
                                if(sock.sendto(data,nexthop_tuple)):
                                    data = f.read(BUFFER)
                            print file_chunk_to_transfer + " chunk " + file_sequence_number + " transferred to next hop " + nexthop
                            f.close()                        
                    functionlist[command](*parsed['address'], **parsed['weight'])       #Call command function
            else:                                               #If no user input, listens for messages from other nodes
                data, sender = sock.recvfrom(BUFFER)            #If there is data, load it
                message = json.loads(data)
                command = message['command']                    #Determine what kind of command the message contains
                if command in ['LINKDOWN', 'LINKUP']:           #If LINKDOWN or LINKUP
                    optional_args = message['optional']         
                    functionlist[command](*sender, **optional_args)     #Call command function
                elif command == 'TRANSFER':                         #If TRANSFER
                    file_name = message['file_chunk_to_transfer']   #Get file_name and sequence_number from message
                    sequence_number = message['sequence_number']
                    file_chunk, sender = sock.recvfrom(BUFFER)  
                    f = open(file_name,'w')                         #Write file to file_name
                    try:
                        while(file_chunk):
                            f.write(file_chunk)
                            sock.settimeout(2)
                            file_chunk, sender = sock.recvfrom(BUFFER)
                    except socket.timeout:
                        f.close()
                    if message['is_destination']:                   #If receiving node is the destination of the file
                        size = os.path.getsize(file_name)           #Print confirmation message with timestamp, file size, path
                        path = message['path']
                        path_string = ''                            #Assemble path in user-friendly format
                        i = 0
                        for hop in path:
                            if i == 0:
                                path_string += hop
                                i += 1
                            else:
                                path_string += ", " + hop
                                i +=1
                        print "<" + str(current_time()) + "> Received file of size " + str(size) + " along path " + path_string
                        file_dict[sequence_number] = file_name          #Add file to file_dict for client
                        if ('1' in file_dict and '2' in file_dict):     #If client has both chunks of file
                            os.system("cat " + file_dict['1'] + " " +  file_dict['2'] + " > output.jpg")        #Cat files together
                            print "All file chunks received. File has been combined and saved as 'output.jpg'"                        
                    else:                                           #If receiving node is not the destination of the file
                        destination = message['destination']        #Determine destination
                        destination_key = '{0}:{1}'.format(destination[0], destination[1])
                        destination_tuple = tuple([destination[0], int(destination[1])])
                        path = message['path']                      #Determine path the file has travelled
                        nexthop = nodes[destination_key]['nexthop'] #Determine next hop to destination
                        nexthop_tuple = tuple([nexthop.split(':')[0], int(nexthop.split(':')[1])])
                        path.append(nexthop)                        #Add next hop to path
                        if nexthop_tuple == destination_tuple:      #If next hop is the destination, set is_destination=True
                            message = json.dumps({'command': command, 'file_chunk_to_transfer': file_name, 'sequence_number': sequence_number, 'destination': destination, 'is_destination': True, 'path': path})
                        else:                                       #Otherwise, set is_destination=False
                            message = json.dumps({'command': command, 'file_chunk_to_transfer': file_name, 'sequence_number': sequence_number, 'destination': destination, 'is_destination': False, 'path': path})
                        sock.sendto(message, nexthop_tuple)         #Send message to next hop
                        f = open(file_name, 'rU')                   #Send file to next hop
                        data = f.read(BUFFER)
                        while (data):
                            if(sock.sendto(data,nexthop_tuple)):
                                data = f.read(BUFFER)    
                        print file_name + " chunk " + sequence_number + " transferred to next hop " + nexthop
                elif command == "ROUTEUPDATE":                      #If message is ROUTEUPDATE
                    optional_args = message['update']           
                    update_costs(*sender, **optional_args)          #Call update_costs function
    sock.close()