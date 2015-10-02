
import socket
import logging
import string
import time
import threading
import multiprocessing 
import Queue
import select
import pakbus

def hexdump(pkt):
    return " ".join(["{:02x}".format(ord(x)) for x in pkt])
#
# Quote PakBus packet
#
def quote(pkt):
    pkt = string.replace(pkt, '\xBC', '\xBC\xDC') # quote \xBC characters
    pkt = string.replace(pkt, '\xBD', '\xBC\xDD') # quote \xBD characters
    return pkt

#
# Unquote PakBus packet
#
def unquote(pkt):
    pkt = string.replace(pkt, '\xBC\xDD', '\xBD') # unquote \xBD characters
    pkt = string.replace(pkt, '\xBC\xDC', '\xBC') # unquote \xBC characters
    return pkt
                 
#
# Calculate signature for PakBus packets
#
def calcSigFor(buff, seed = 0xAAAA):
    sig = seed
    for x in buff:
        x = ord(x)
        j = sig
        sig = (sig <<1) & 0x1FF
        if sig >= 0x100: sig += 1
        sig = ((((sig + (j >>8) + x) & 0xFF) | (j <<8))) & 0xFFFF
    return sig
    
def calcSigNullifier(sig):
    nulb = nullif = ''
    for i in 1,2:
        sig = calcSigFor(nulb, sig)
        sig2 = (sig<<1) & 0x1FF
        if sig2 >= 0x100: sig2 += 1
        nulb = chr((0x100 - (sig2 + (sig >>8))) & 0xFF)
        nullif += nulb
    return nullif
        
        

class PakReader():
    NO_PKT = 0
    BEGIN_PKT = 1
    IN_PKT = 2
    
    def __init__(self, onpkt):
        self.data = ''
        self.state = PakReader.NO_PKT
        self.onpkt = onpkt
        
    def recv (self, pkt):
        "append bytes to data looking for packet boundaries"
        
        for b in pkt:
            if self.state == PakReader.NO_PKT:
                if b == '\xBD':
                    self.state = PakReader.BEGIN_PKT
                    
            if self.state == PakReader.BEGIN_PKT:
                if b != '\xBD':
                    self.state = PakReader.IN_PKT
                    self.data = ''
                    
            if self.state == PakReader.IN_PKT:
                if b != '\xBD':
                    self.data += b
                else:
                    if len(self.data) > 1:                             
                        self.onpkt(unquote(self.data))
                    self.state = PakReader.NO_PKT            
            
        
class PakSock(threading.Thread):
    """pakbus socket class with robust reconnection
    
    and useful pakbus processing
    
    """
    
    def __init__(self, host, port, timeout, my_node_id):
        threading.Thread.__init__(self)
        self.name = "Pakbus background comm. thread"
        self.setDaemon(True)
        self.host = host
        self.port = port
        self.tx_queue = multiprocessing.Queue()
        self.rx_queue = multiprocessing.Queue()
        self.my_node_id = my_node_id
        
        self.have_socket_evt = multiprocessing.Event()
        self.shutdown_evt = multiprocessing.Event()        
        self.shutdown_complete_evt = multiprocessing.Event()
        self.log = logging.getLogger("paksock")    
        self.timeout = timeout
        self.socket = None
        
        self.start()
        
    def onPacket(self, pkt):        
        if calcSigFor(pkt):
            self.log.warn("  <- Rx [{:3}] with bad signature: {}".format(len(pkt), hexdump(pkt)))
        else:            
            self.log.debug("  Rx [{:3}]: {}".format(len(pkt), hexdump(pkt)))    
            #self.rx_queue.put(pkt[:-2]) # trim signature off
            
            hdr, msg = pakbus.decode_pkt(pkt[:-2])
            
            self.log.debug("    Head: {}".format(hdr))
            self.log.debug("    Msg: {}".format(msg))
                    
            # Respond to incoming hello command packets
            if msg['MsgType'] == 0x09 and hdr['DstNodeId'] == self.my_node_id:
                self.log.info(" Responding to HELLO")
                pkt = pakbus.pkt_hello_response(hdr['SrcNodeId'], self.my_node_id, msg['TranNbr'])
                self.send(pkt)
            else:            
                self.rx_queue.put( (hdr, msg) )
        
    def run(self):
        self.shutdown_evt.clear()
        self.shutdown_complete_evt.clear()
        
        prd = PakReader(self.onPacket)
        while not self.shutdown_evt.is_set():
            self.have_socket_evt.clear()   
            s = None               
            # get a socket.
            for res in socket.getaddrinfo(self.host, 
                                          self.port, 
                                          socket.AF_UNSPEC, socket.SOCK_STREAM):            
                self.res = res
                s = self.open()
                
                if s:
                    break
        
            if s == None:
                self.log.warn("Unable to open socket!")
                time.sleep(1)
            else:
                self.have_socket_evt.set()
                
                while not self.shutdown_evt.is_set():
                    # operate on socket
                    #self.shutdown_evt,
                    #self.shutdown_evt._flag
                    rlist, wlist, xlist = select.select([ 
                                                         self.tx_queue._reader, 
                                                         s], 
                                                        [], 
                                                        [])
                    
                    if self.shutdown_evt._flag in rlist:
                        self.log.info("Got shutdown request")
                    try:  
                        if self.tx_queue._reader in rlist:                        
                            x = self.tx_queue.get()
                            self.log.debug("  Tx [{:3}]: {}".format(len(x), hexdump(x)))
                            s.sendall(x)
                            
                        if s in rlist:
                            # receive
                            b = s.recv(2048)
                            prd.recv(b)             
                    except ValueError as vmsg:
                        logging.error("Decoding packet failed: {}".format(msg))
                        break  # break reconnects socket                        
                    except socket.error as msg:
                        logging.error("Socket died with: {}".format(msg))
                        break # break reconnects scoket
            
                        
        self.shutdown_complete_evt.set()
        self.log.warn("thread shutdown")
            
    def have_socket(self):
        return self.have_socket_evt.is_set()
    
    def send_and_wait(self, pkt, DstNodeId, SrcNodeId, TranNbr, retries = 10):
    
        hdr = {}
        msg = {}
        for t in range(retries):
            self.send(pkt)
            hdr, msg = self.wait_pkt(DstNodeId, SrcNodeId, TranNbr, self.timeout / retries)
            
            if not(hdr == {} and msg == {}):
                break

        return hdr, msg
            
    def send(self, pkt):
        #
        # Send packet over PakBus
        #
        # - add signature nullifier
        # - quote \xBC and \xBD characters
        # - frame packet with \xBD characters
        #        
        "quote, add sig nullifier, and frame with \xBD chars"   
     
        # pkt: unquoted, unframed PakBus packet (just header + message)
        frame = quote(pkt + calcSigNullifier(calcSigFor(pkt)))
        self.tx_queue.put('\xBD' + frame + '\xBD')
        
                
    def shutdown(self):
        #self.shutdown_evt.set()
        #self.join(5)
        if self.socket:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        self.socket = None
                    
    def open(self):
        if self.socket:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
        self.socket = None
        
        af, socktype, proto, canonname, sa = self.res

        self.log.info("Opening pakbus connection to {} at tcp://{}:{}".format(self.host,
                                                                              sa[0], sa[1]))
        try:
            s = socket.socket(af, socktype, proto)
        except socket.error, msg:
            self.log.info("  failed: {}".format(msg))
            s = None
       
        if s:
            try:
                # Set timeout and try to connect to socket
                #s.settimeout(self.timeout)
                s.setblocking(1)
                s.connect(sa)
                s.setblocking(0)
            except KeyError:
                s.close()
                s = None
           
            except Exception as x:
                self.log.info("  error opening socket: {}".format(x))
                s = None
       
           
        return s    
    
    
    def wait_pkt(self, SrcNodeId, DstNodeId, TranNbr, timeout = None):
        """Wait for an incoming packet
        
        """
    
        # SrcNodeId:    source node ID (12-bit int)
        # DstNodeId:    destination node ID (12-bit int)
        # TranNbr:      expected transaction number
        # timeout:      timeout in seconds
        
        if not timeout:
            timeout = self.timeout    
        
        while True:
            try:
                hdr, msg = self.rx_queue.get(True, timeout)
                
                #hdr, msg = pakbus.decode_pkt(x)
                
                # ignore packets that are not for us
                if hdr['DstNodeId'] != DstNodeId or hdr['SrcNodeId'] != SrcNodeId:
                    continue
        
                # Handle "please wait" packets
                if msg['TranNbr'] == TranNbr and msg['MsgType'] == 0xa1:
                    timeout = msg['WaitSec']                    
                    continue
        
                # this should be the packet we are waiting for
                if msg['TranNbr'] == TranNbr:
                    return hdr, msg
                
            except Queue.Empty:
                return {}, {}
                break
    
        return {}, {}
    
                