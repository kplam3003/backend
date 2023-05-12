import gevent
import gevent.monkey
gevent.monkey.patch_socket()

from flask import Flask, request, jsonify

def task(t):
    print(f"Begin: {t}")
    gevent.sleep(5)
    print(t)
    print(f"End: {t}")

if __name__ == "__main__":
    t1 = gevent.spawn(task, "Mot")
    t2 = gevent.spawn(task, "Hai")
    t3 = gevent.spawn(task, "Ba")
    gevent.joinall([t1, t2, t3])




