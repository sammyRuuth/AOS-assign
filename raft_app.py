# raft_app.py — Final version with replicated login + commit-sync fix
import threading, time, random, sys, uuid
from concurrent import futures
import grpc
import disticket_pb2, disticket_pb2_grpc


# ---------------- Helper ----------------
def parse_peers(peers_str):
    return [p.strip() for p in peers_str.split(",") if p.strip()]


# ---------------- Raft Node ----------------
class RaftNode(disticket_pb2_grpc.RaftServicer):
    def __init__(self, node_id, host_port, peers, app_state):
        self.node_id = node_id
        self.host_port = host_port
        self.peers = peers
        self.app_state = app_state

        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        self.commitIndex = -1
        self.lastApplied = -1
        self.nextIndex = {}
        self.matchIndex = {}

        self.role = "follower"
        self.leaderId = None
        self.lock = threading.Lock()

        self.election_timeout = self.random_election_timeout()
        self.last_heartbeat = time.time()

        self.stubs = {}
        threading.Thread(target=self.ticker, daemon=True).start()

    def random_election_timeout(self):
        return random.uniform(1.0, 2.0)

    # ---------------- RPC: RequestVote ----------------
    def RequestVote(self, request, context):
        with self.lock:
            resp = disticket_pb2.RequestVoteResp(term=self.currentTerm, voteGranted=False)
            if request.term < self.currentTerm:
                return resp
            if request.term > self.currentTerm:
                self.currentTerm = request.term
                self.votedFor = None
                self.role = "follower"
            my_last_index = len(self.log) - 1
            my_last_term = self.log[my_last_index]["term"] if my_last_index >= 0 else 0
            up_to_date = (request.lastLogTerm > my_last_term) or (
                request.lastLogTerm == my_last_term and request.lastLogIndex >= my_last_index
            )
            if (self.votedFor is None or self.votedFor == request.candidateId) and up_to_date:
                self.votedFor = request.candidateId
                resp.voteGranted = True
                self.last_heartbeat = time.time()
            resp.term = self.currentTerm
            return resp

    # ---------------- RPC: AppendEntries ----------------
    def AppendEntries(self, request, context):
        with self.lock:
            resp = disticket_pb2.AppendEntriesResp(term=self.currentTerm, success=False, matchIndex=-1)
            if request.term < self.currentTerm:
                return resp
            self.leaderId = request.leaderId
            self.currentTerm = request.term
            self.role = "follower"
            self.last_heartbeat = time.time()

            prevIndex = request.prevLogIndex
            prevTerm = request.prevLogTerm
            if prevIndex >= 0:
                if prevIndex >= len(self.log) or self.log[prevIndex]["term"] != prevTerm:
                    return resp
            insert_at = prevIndex + 1
            while len(self.log) > insert_at:
                self.log.pop()
            for e in request.entries:
                self.log.append({"term": e.term, "command": e.command, "requestId": e.requestId})
            resp.success = True
            resp.matchIndex = len(self.log) - 1
            if request.leaderCommit > self.commitIndex:
                self.commitIndex = min(request.leaderCommit, len(self.log) - 1)
            self.apply_entries()
            resp.term = self.currentTerm
            return resp

    # ---------------- Internal helpers ----------------
    def get_stub(self, peer):
        if peer in self.stubs:
            return self.stubs[peer]
        ch = grpc.insecure_channel(peer)
        stub = disticket_pb2_grpc.RaftStub(ch)
        self.stubs[peer] = stub
        return stub

    # ---------------- Election Loop ----------------
    def ticker(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                now = time.time()
                if self.role == "leader":
                    self.broadcast_append_entries(is_heartbeat=True)
                    time.sleep(0.2)
                else:
                    if now - self.last_heartbeat >= self.election_timeout:
                        self.start_election()

    def start_election(self):
        self.currentTerm += 1
        self.role = "candidate"
        self.votedFor = self.host_port
        votes = 1
        last_index = len(self.log) - 1
        last_term = self.log[last_index]["term"] if last_index >= 0 else 0
        peers = list(self.peers)
        self.lock.release()

        def ask(peer):
            nonlocal votes
            try:
                stub = self.get_stub(peer)
                req = disticket_pb2.RequestVoteReq(
                    term=self.currentTerm,
                    candidateId=self.host_port,
                    lastLogIndex=last_index,
                    lastLogTerm=last_term,
                )
                r = stub.RequestVote(req, timeout=1)
                if r.voteGranted:
                    votes += 1
            except Exception:
                pass

        threads = []
        for p in peers:
            if p == self.host_port:
                continue
            t = threading.Thread(target=ask, args=(p,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        self.lock.acquire()
        if votes > (len(self.peers) // 2):
            self.role = "leader"
            self.leaderId = self.host_port
            for p in self.peers:
                self.nextIndex[p] = len(self.log)
                self.matchIndex[p] = -1
            print(f"[{self.host_port}] became LEADER (term {self.currentTerm})")
        else:
            self.role = "follower"
        self.last_heartbeat = time.time()
        self.election_timeout = self.random_election_timeout()

    # ---------------- AppendEntries Broadcast ----------------
    def broadcast_append_entries(self, is_heartbeat=False):
        for p in self.peers:
            if p == self.host_port:
                continue
            try:
                stub = self.get_stub(p)
                next_idx = self.nextIndex.get(p, len(self.log))
                prevIndex = next_idx - 1
                prevTerm = self.log[prevIndex]["term"] if 0 <= prevIndex < len(self.log) else 0
                entries = []
                if not is_heartbeat and next_idx < len(self.log):
                    for i in range(next_idx, len(self.log)):
                        e = self.log[i]
                        entries.append(
                            disticket_pb2.LogEntry(
                                term=e["term"], command=e["command"], requestId=e["requestId"]
                            )
                        )
                req = disticket_pb2.AppendEntriesReq(
                    term=self.currentTerm,
                    leaderId=self.host_port,
                    prevLogIndex=prevIndex,
                    prevLogTerm=prevTerm,
                    entries=entries,
                    leaderCommit=self.commitIndex,
                )
                r = stub.AppendEntries(req, timeout=1)
                if r.success:
                    self.nextIndex[p] = r.matchIndex + 1
                    self.matchIndex[p] = r.matchIndex
                else:
                    self.nextIndex[p] = max(0, self.nextIndex.get(p, len(self.log)) - 1)
            except Exception:
                pass

    # ---------------- State Machine ----------------
    def apply_entries(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            e = self.log[self.lastApplied]
            parts = e["command"].split(":")
            cmd = parts[0]
            if cmd == "BOOK":
                seat = int(parts[1])
                user = parts[2]
                self.app_state.apply_booking(seat, user, e["requestId"])
            elif cmd == "LOGIN":
                user = parts[1]
                token = parts[2]
                self.app_state.apply_login(user, token)
            elif cmd == "LOGOUT":
                token = parts[1]
                self.app_state.apply_logout(token)

    # ---------------- Client Proposal ----------------
    def propose(self, command, request_id):
        with self.lock:
            if self.role != "leader":
                return False, "NOT_LEADER", self.leaderId or self.host_port
            entry = {"term": self.currentTerm, "command": command, "requestId": request_id}
            self.log.append(entry)
            self.broadcast_append_entries(is_heartbeat=False)
            start = time.time()
            while time.time() - start < 2:
                matches = 1
                for p in self.peers:
                    if p == self.host_port:
                        continue
                    if self.matchIndex.get(p, -1) >= len(self.log) - 1:
                        matches += 1
                if matches > (len(self.peers) // 2):
                    self.commitIndex = len(self.log) - 1
                    self.apply_entries()
                    return True, "OK", None
                time.sleep(0.05)
            return False, "REPLICATION_TIMEOUT", None


# ---------------- App State ----------------
class AppState:
    def __init__(self, total_seats=10):
        self.total_seats = total_seats
        self.seats = {i: {"booked": False, "by": "", "requestId": ""} for i in range(1, total_seats + 1)}
        self.tokens = {}
        self.lock = threading.Lock()

    def apply_login(self, user, token):
        self.tokens[token] = user

    def apply_logout(self, token):
        if token in self.tokens:
            del self.tokens[token]

    def apply_booking(self, seat_no, user, request_id):
        with self.lock:
            if seat_no < 1 or seat_no > self.total_seats:
                return False
            s = self.seats[seat_no]
            if s["booked"]:
                return False
            s.update({"booked": True, "by": user, "requestId": request_id})
            return True

    def get_availability(self):
        with self.lock:
            return [{"seat_no": n, "booked": v["booked"], "by": v["by"]} for n, v in self.seats.items()]


# ---------------- AppAPI Service ----------------
class AppAPIService(disticket_pb2_grpc.AppAPIServicer):
    def __init__(self, raftnode, app_state):
        self.raftnode = raftnode
        self.app_state = app_state

    def Login(self, request, context):
        user = request.username
        token = str(uuid.uuid4())
        request_id = str(uuid.uuid4())

        success, msg, leader = self.raftnode.propose(f"LOGIN:{user}:{token}", request_id)

        if success:
            # Wait briefly until committed and applied
            time.sleep(0.3)
            if token in self.app_state.tokens:
                return disticket_pb2.LoginResponse(ok=True, token=token, status="OK")
            else:
                return disticket_pb2.LoginResponse(ok=False, token="", status="Commit wait timeout")

        if msg == "NOT_LEADER" and leader:
            return disticket_pb2.LoginResponse(ok=False, token="", status=f"NOT_LEADER:{leader}")
        return disticket_pb2.LoginResponse(ok=False, token="", status=msg)

    def Logout(self, request, context):
        token = request.token
        request_id = str(uuid.uuid4())
        success, msg, leader = self.raftnode.propose(f"LOGOUT:{token}", request_id)
        if success:
            return disticket_pb2.Status(ok=True, message="Logged out")
        if msg == "NOT_LEADER" and leader:
            return disticket_pb2.Status(ok=False, message=f"NOT_LEADER:{leader}")
        return disticket_pb2.Status(ok=False, message=msg)

    def GetAvailability(self, request, context):
        seats = self.app_state.get_availability()
        resp = disticket_pb2.GetResponse(ok=True, message="OK")
        for s in seats:
            resp.seats.add(seat_no=s["seat_no"], booked=s["booked"], by=s["by"])
        return resp

    def PostBooking(self, request, context):
        token = request.token
        user = self.app_state.tokens.get(token)
        if not user or user != request.user:
            return disticket_pb2.Status(ok=False, message="Auth fail")
        seat = request.seat_no
        success, msg, leader = self.raftnode.propose(f"BOOK:{seat}:{user}", request.request_id)
        if success:
            return disticket_pb2.Status(ok=True, message="BOOKED")
        if msg == "NOT_LEADER" and leader:
            return disticket_pb2.Status(ok=False, message=f"NOT_LEADER:{leader}")
        return disticket_pb2.Status(ok=False, message=msg)


# ---------------- Server Runner ----------------
def serve(node_id, host_port, peers, total_seats=10):
    app_state = AppState(total_seats)
    raftnode = RaftNode(node_id, host_port, peers, app_state)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    disticket_pb2_grpc.add_RaftServicer_to_server(raftnode, server)
    disticket_pb2_grpc.add_AppAPIServicer_to_server(AppAPIService(raftnode, app_state), server)
    server.add_insecure_port(host_port)
    server.start()
    print(f"[{node_id}] Server started on {host_port}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)


# ---------------- CLI ----------------
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python raft_app.py <node_id> <host:port> <peer1,peer2,...> [total_seats]")
        sys.exit(1)
    node_id = sys.argv[1]
    host_port = sys.argv[2]
    peers = parse_peers(sys.argv[3])
    total_seats = int(sys.argv[4]) if len(sys.argv) > 4 else 10
    serve(node_id, host_port, peers, total_seats)
