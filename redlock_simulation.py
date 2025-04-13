import redis
import time
import uuid
import multiprocessing

client_processes_waiting = [0, 1, 1, 1, 4]


class Redlock:
    def __init__(self, redis_nodes):
        self.servers = []
        for host, port in redis_nodes:
            self.servers.append(
                redis.StrictRedis(
                    host=host,
                    port=port,
                    socket_timeout=5000,  # 5-second timeout
                    retry_on_timeout=True
                )
            )
        self.quorum = len(self.servers) // 2 + 1

    def acquire_lock(self, resource, ttl, retries=3, retry_delay=100):
        lock_id = str(uuid.uuid4())
        for attempt in range(retries):
            acquired = 0
            start_time = time.time() * 1000
            for server in self.servers:
                try:
                    if server.set(resource, lock_id, nx=True, px=ttl):
                        acquired += 1
                except redis.exceptions.RedisError:
                    continue

            elapsed = (time.time() * 1000) - start_time
            remaining_ttl = ttl - elapsed
            min_validity = ttl * 0.5  # Half the TTL (clock drift safety)

            if acquired >= self.quorum and remaining_ttl > min_validity:
                return True, lock_id
            else:
                self.release_lock(resource, lock_id)
                if attempt < retries - 1:
                    time.sleep(retry_delay / 1000)  # Convert ms to seconds
        return False, None

    def release_lock(self, resource, lock_id):
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        for server in self.servers:
            try:
                server.eval(release_script, 1, resource, lock_id)
            except redis.exceptions.RedisError:
                continue
            
        

def client_process(redis_nodes, resource, ttl, client_id):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id])

    redlock = Redlock(redis_nodes)
    print(f"\nClient-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

    if lock_acquired:
        print(f"\nClient-{client_id}: Lock acquired! Lock ID: {lock_id}")
        # Simulate critical section
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id)
        print(f"\nClient-{client_id}: Lock released!")
    else:
        print(f"\nClient-{client_id}: Failed to acquire lock.")

if __name__ == "__main__":
    # Define Redis node addresses (host, port)
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
