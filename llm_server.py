# llm_server.py
import time, sys
from concurrent import futures
import grpc
import disticket_pb2, disticket_pb2_grpc

class LLMServicer(disticket_pb2_grpc.LLMServiceServicer):
    def GetLLMAnswer(self, request, context):
        q = request.query.lower()
        if "cancel" in q:
            ans = "To cancel, go to your bookings and click cancel. Refunds (mock) processed within 5 business days."
        elif "available" in q or "seats" in q:
            ans = "Use GetAvailability RPC to check seats. Seats are numbered 1..N and replicated across nodes."
        else:
            ans = "Sorry, I don't know. (This is a mock LLM.)"
        return disticket_pb2.LLMResponse(requestId=request.requestId, answer=ans)

def serve(port="50061"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    disticket_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"LLM server listening on {port}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    port = sys.argv[1] if len(sys.argv) > 1 else "50061"
    serve(port)
