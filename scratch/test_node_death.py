"""Probe: with max_restarts=-1 + NodeAffinity(soft=False), what does the
caller see when the pinned node dies?

Runs inside the head Ray container. Assumes a worker container has already
joined the cluster (see run_node_death_test.sh). Once the actor is pinned
to the worker and confirmed alive, the harness kills the worker container
externally; this script keeps polling the actor and reports the state
transitions it observes.
"""
import time
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.exceptions import (
    ActorDiedError,
    ActorUnavailableError,
    GetTimeoutError,
)

ray.init(address="auto")

# Discover the worker node (anything not the head we run on).
head_node_id = ray.get_runtime_context().get_node_id()
worker_nodes = [
    n for n in ray.nodes()
    if n["Alive"] and n["NodeID"] != head_node_id
]
assert worker_nodes, (
    "no worker node found. Start a worker container joining this head "
    "before running the test."
)
worker = worker_nodes[0]
worker_node_id = worker["NodeID"]
print(f"head node    : {head_node_id}")
print(f"worker node  : {worker_node_id}")
print(f"worker addr  : {worker.get('NodeManagerAddress')}")


@ray.remote(max_restarts=-1)
class Pinned:
    def __init__(self):
        self._node_id = ray.get_runtime_context().get_node_id()

    def hello(self) -> str:
        return f"alive on {self._node_id}"


actor = Pinned.options(
    name="test-pinned",
    namespace="node-death",
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        worker_node_id, soft=False,
    ),
).remote()

# Confirm actor is up on the worker.
print(f"\n[pre-kill] {ray.get(actor.hello.remote(), timeout=30)}")

print("\n===== waiting for external `docker kill worker` =====")
print("(the run_node_death_test.sh harness triggers it 10s after this line)")


def probe(label: str, call_timeout: float = 10):
    start = time.time()
    try:
        r = ray.get(actor.hello.remote(), timeout=call_timeout)
        print(f"  [{label}] ok in {time.time() - start:.2f}s -> {r}")
        return "ok"
    except ActorDiedError as e:
        print(f"  [{label}] ActorDiedError in {time.time() - start:.2f}s")
        print(f"           msg[:250]: {str(e)[:250]}")
        return "died"
    except ActorUnavailableError as e:
        print(f"  [{label}] ActorUnavailableError in {time.time() - start:.2f}s")
        print(f"           msg[:250]: {str(e)[:250]}")
        return "unavailable"
    except GetTimeoutError:
        print(f"  [{label}] GetTimeoutError in {time.time() - start:.2f}s "
              f"(call still hanging)")
        return "timeout"
    except Exception as e:
        print(f"  [{label}] {type(e).__name__} in {time.time() - start:.2f}s")
        print(f"           msg[:250]: {str(e)[:250]}")
        return type(e).__name__


# Poll at increasing granularity. If Ray ever escalates from unavailable to
# died, we'll catch it. Otherwise we'll confirm it stays unavailable for the
# full window.
print("\n===== observing actor state over 10 minutes =====")
transitions = []
prev_state = None
t0 = time.time()
for i in range(60):
    state = probe(f"t+{time.time() - t0:5.1f}s")
    if state != prev_state:
        transitions.append((time.time() - t0, state))
        prev_state = state
    time.sleep(10)

print("\n===== state transitions =====")
for t, s in transitions:
    print(f"  t+{t:6.1f}s -> {s}")

print("\n===== done =====")