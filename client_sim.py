# client_sim.py — updated: handles NOT_LEADER for Login + booking redirects
import sys
import time
import uuid
import grpc
import disticket_pb2, disticket_pb2_grpc

MAX_RETRIES = 3

def login_once(addr, username, password, timeout=2.0):
    """One attempt to login at addr. Returns (ok, token, status)."""
    try:
        channel = grpc.insecure_channel(addr)
        stub = disticket_pb2_grpc.AppAPIStub(channel)
        r = stub.Login(disticket_pb2.LoginRequest(username=username, password=password), timeout=timeout)
        return r.ok, r.token, r.status
    except Exception as e:
        return False, "", f"RPC_ERROR:{e}"

def login(addr, username, password):
    """Login but follow NOT_LEADER:<host:port> redirects up to MAX_RETRIES."""
    tried = set()
    current = addr
    for attempt in range(MAX_RETRIES):
        if current in tried:
            break
        tried.add(current)
        print(f"[client] trying login on {current} (attempt {attempt+1})")
        ok, token, status = login_once(current, username, password)
        if ok and token:
            print(f"[client] login succeeded on {current}, token {token}")
            return token, None  # token, leader_hint None
        # handle NOT_LEADER:<leaderAddr>
        if isinstance(status, str) and status.startswith("NOT_LEADER:"):
            leader = status.split(":", 1)[1]
            leader = leader.strip()
            print(f"[client] redirect to leader {leader}")
            current = leader
            continue
        # other failure; try next (no known leader) — try original addr again or fail
        print(f"[client] login failed on {current}: {status}")
        time.sleep(0.2)
    return "", "LOGIN_FAILED"

def get_availability(addr, token):
    try:
        channel = grpc.insecure_channel(addr)
        stub = disticket_pb2_grpc.AppAPIStub(channel)
        r = stub.GetAvailability(disticket_pb2.GetRequest(token=token, queryType="seats"))
        return r
    except Exception as e:
        print("[client] GetAvailability RPC error:", e)
        return None

def post_booking_once(addr, token, user, seat_no, timeout=2.0):
    try:
        channel = grpc.insecure_channel(addr)
        stub = disticket_pb2_grpc.AppAPIStub(channel)
        req_id = str(uuid.uuid4())
        r = stub.PostBooking(disticket_pb2.BookingRequest(token=token, seat_no=seat_no, user=user, request_id=req_id), timeout=timeout)
        return r
    except Exception as e:
        class R:
            ok = False
            message = f"RPC_ERROR:{e}"
        return R()

def post_booking(addr, token, user, seat_no):
    """Post booking; if redirected to leader, retry there."""
    r = post_booking_once(addr, token, user, seat_no)
    if r.ok:
        return r
    if isinstance(r.message, str) and r.message.startswith("NOT_LEADER:"):
        leader = r.message.split(":",1)[1]
        leader = leader.strip()
        print(f"[client] booking redirected to leader {leader} — retrying")
        r2 = post_booking_once(leader, token, user, seat_no)
        return r2
    return r

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 client_sim.py <app_addr> <username> <seat_no>")
        sys.exit(1)

    app_addr = sys.argv[1]
    user = sys.argv[2]
    seat = int(sys.argv[3])

    # 1) Login (follows redirects to leader)
    token, err = login(app_addr, user, "x")
    if not token:
        print("Logged in, token", token)
        print("Login failed:", err)
        sys.exit(1)

    # 2) Try booking on the originally contacted node (it may redirect)
    resp = post_booking(app_addr, token, user, seat)
    print("Booking response:", resp.ok, resp.message)
    if not resp.ok and isinstance(resp.message, str) and resp.message.startswith("NOT_LEADER:"):
        leader = resp.message.split(":",1)[1]
        print("Retrying on leader", leader)
        resp2 = post_booking(leader, token, user, seat)
        print("Leader booking response:", resp2.ok, resp2.message)

    # 3) Print availability from the node we contacted originally
    g = get_availability(app_addr, token)
    if g:
        print("Availability snapshot from contacted node:")
        for s in g.seats:
            print(f"seat_no: {s.seat_no}\n")
