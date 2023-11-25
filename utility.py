from threading import Thread
import socket
import select

def run_thread(target, args):
    thread = Thread(target=target, args=args)
    thread.daemon = True
    thread.start
    return thread

def startup_server(ip, port):
    while True:
        try:
            serv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            serv_socket.setsockopt
            serv_socket.connect((str(ip), int(port)))
            return serv_socket
        except Exception as e:
            print(e)


def send_and_recv_no_retry(msg, ip, port, timeout=-1):    
    conn = startup_server(ip, port)
    resp = None
    try:
        conn.sendall(msg.encode())
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode()
        else:
            resp = conn.recv(2048).decode()
    except Exception as e:
        print(e)
    
    conn.close()
    return resp
            
def send_and_recv(msg, ip, port, res=None, timeout=-1):
    resp = None
    while True:
        resp = send_and_recv_no_retry(msg, ip, port, timeout)    
        if resp:
            break
    if res is not None:
        res.put(resp)
    return resp
