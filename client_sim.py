# client_sim.py — Fixed version
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
            return token, None
        # handle NOT_LEADER:<leaderAddr>
        if isinstance(status, str) and status.startswith("NOT_LEADER:"):
            leader = status.split(":", 1)[1]
            leader = leader.strip()
            print(f"[client] redirect to leader {leader}")
            current = leader
            continue
        # other failure
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
        r = stub.PostBooking(disticket_pb2.BookingRequest(
            token=token, seat_no=seat_no, user=user, request_id=req_id
        ), timeout=timeout)
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
        leader = r.message.split(":", 1)[1].strip()
        print(f"[client] booking redirected to leader {leader} — retrying")
        r2 = post_booking_once(leader, token, user, seat_no)
        return r2
    return r

def query_llm(llm_addr, query):
    """Query the LLM server for customer support."""
    try:
        channel = grpc.insecure_channel(llm_addr)
        stub = disticket_pb2_grpc.LLMServiceStub(channel)
        req_id = str(uuid.uuid4())
        r = stub.GetLLMAnswer(disticket_pb2.LLMRequest(requestId=req_id, query=query))
        return r.answer
    except Exception as e:
        return f"LLM Error: {e}"

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 client_sim.py <app_addr> <username> <seat_no> [llm_addr]")
        sys.exit(1)

    app_addr = sys.argv[1]
    user = sys.argv[2]
    seat = int(sys.argv[3])
    llm_addr = sys.argv[4] if len(sys.argv) > 4 else "localhost:50061"

    # 1) Login (follows redirects to leader)
    print("\n=== LOGIN ===")
    token, err = login(app_addr, user, "password123")
    if not token:
        print(f"❌ Login failed: {err}")
        sys.exit(1)
    print(f"✓ Logged in successfully, token: {token[:8]}...")

    # 2) Check availability before booking
    print("\n=== CHECKING AVAILABILITY ===")
    avail = get_availability(app_addr, token)
    if avail and avail.ok:
        print("Current seat status:")
        for s in avail.seats:
            status = f"BOOKED by {s.by}" if s.booked else "AVAILABLE"
            print(f"  Seat {s.seat_no}: {status}")
    
    # 3) Try booking
    print(f"\n=== BOOKING SEAT {seat} ===")
    resp = post_booking(app_addr, token, user, seat)
    if resp.ok:
        print(f"✓ Booking successful: {resp.message}")
    else:
        print(f"❌ Booking failed: {resp.message}")

    # 4) Query LLM for help
    print("\n=== LLM CUSTOMER SUPPORT ===")
    questions = [
        "How do I cancel a booking?",
        "What seats are available?",
        "What is your refund policy?"
    ]
    for q in questions:
        answer = query_llm(llm_addr, q)
        print(f"Q: {q}")
        print(f"A: {answer}\n")

    # 5) Final availability check
    print("=== FINAL AVAILABILITY ===")
    avail = get_availability(app_addr, token)
    if avail and avail.ok:
        available_count = sum(1 for s in avail.seats if not s.booked)
        print(f"Available seats: {available_count}/{len(avail.seats)}")
        for s in avail.seats:
            if s.booked:
                print(f"  Seat {s.seat_no}: BOOKED by {s.by}")