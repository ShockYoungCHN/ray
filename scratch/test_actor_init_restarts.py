"""Probe how Ray behaves when __init__ raises with max_restarts=-1 vs 0."""
import time
import ray
from ray.exceptions import ActorDiedError, ActorUnavailableError, GetTimeoutError

ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)


def probe(max_restarts, label):
    print(f"\n===== {label} (max_restarts={max_restarts}) =====")

    @ray.remote(max_restarts=max_restarts)
    class Bad:
        def __init__(self):
            print(f"  [actor] __init__ running, will raise")
            raise RuntimeError("intentional init failure")

        def hello(self):
            return 1

    b = Bad.remote()

    start = time.time()
    try:
        # Use timeout to avoid hanging the whole test if infinite retry happens
        result = ray.get(b.hello.remote(), timeout=15.0)
        print(f"  [caller] got result={result} (unexpected)")
    except ActorDiedError as e:
        elapsed = time.time() - start
        print(f"  [caller] ActorDiedError after {elapsed:.2f}s")
        print(f"           type(e) = {type(e).__name__}")
        print(f"           e.__cause__ = {type(e.__cause__).__name__ if e.__cause__ else None}")
        print(f"           e.__context__ = {type(e.__context__).__name__ if e.__context__ else None}")
        # Explore all attributes
        for attr in ("actor_id", "cause", "error_type", "error_message",
                     "creation_task_error", "task_id", "ip_address"):
            if hasattr(e, attr):
                val = getattr(e, attr)
                print(f"           e.{attr} = {type(val).__name__}: {str(val)[:120]}")
        # Full message
        print(f"           full str(e):\n{str(e)}")
    except ActorUnavailableError as e:
        elapsed = time.time() - start
        print(f"  [caller] ActorUnavailableError after {elapsed:.2f}s")
        print(f"           message: {str(e)[:200]}")
    except GetTimeoutError:
        elapsed = time.time() - start
        print(f"  [caller] GetTimeoutError after {elapsed:.2f}s (call hung — Ray retrying init?)")
    except Exception as e:
        elapsed = time.time() - start
        print(f"  [caller] {type(e).__name__} after {elapsed:.2f}s: {str(e)[:200]}")

    # If it timed out, try a second call to see what state the actor is in
    print(f"  [caller] second call to check state...")
    start = time.time()
    try:
        ray.get(b.hello.remote(), timeout=5.0)
        print(f"  [caller] second call succeeded (unexpected)")
    except ActorDiedError as e:
        elapsed = time.time() - start
        print(f"  [caller] 2nd call: ActorDiedError after {elapsed:.2f}s")
    except ActorUnavailableError as e:
        elapsed = time.time() - start
        print(f"  [caller] 2nd call: ActorUnavailableError after {elapsed:.2f}s (still restarting)")
    except GetTimeoutError:
        elapsed = time.time() - start
        print(f"  [caller] 2nd call: GetTimeoutError after {elapsed:.2f}s (still hanging)")
    except Exception as e:
        elapsed = time.time() - start
        print(f"  [caller] 2nd call: {type(e).__name__} after {elapsed:.2f}s")


probe(max_restarts=0, label="Case A")
probe(max_restarts=-1, label="Case B")
probe(max_restarts=3, label="Case C (finite)")

print("\n===== done =====")
