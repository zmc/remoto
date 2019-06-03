from remoto.process import StopCallback

def reporting(conn, result, timeout=None):
    timeout = timeout or conn.global_timeout # -1 a.k.a. wait for ever

    while True:
        try:
            conn.report(result, timeout)
        except StopCallback:
            break
