"""Probe what caller sees when an actor method fails vs when the actor process
dies mid-life (os._exit), under max_restarts=-1.

We want to verify:
  1. Python-level exception in a method (RuntimeError, MemoryError) — actor
     process stays alive; caller sees RayTaskError. NOT ActorDiedError.
  2. Method calls os._exit(1) directly — process dies; Ray auto-restarts.
     Caller of the in-flight call sees what? Subsequent callers see what?
  3. Background thread inside actor calls os._exit(1) — process dies mid-flight;
     any concurrent .remote() calls surface what?

This validates the assumption in our shuffle runtime that ActorDiedError with
max_restarts=-1 only fires on init failure or explicit ray.kill — never for
mid-life exceptions or process death (which route through ActorUnavailableError
during Ray's automatic restart).
"""
import os
import threading
import time
import ray
from ray.exceptions import (
    ActorDiedError,
    ActorUnavailableError,
    RayTaskError,
    GetTimeoutError,
)

ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)


@ray.remote(max_restarts=-1)
class Probe:
    def __init__(self):
        self._alive = True

    def hello(self) -> str:
        return "ok"

    def raise_runtime(self):
        raise RuntimeError("intentional method error")

    def raise_memory(self):
        raise MemoryError("intentional MemoryError")

    def suicide_exit(self):
        # Kill the actor process from inside the method.
        os._exit(1)

    def suicide_from_thread(self):
        # Spawn a thread that kills the process after a short delay, then
        # return so this method call itself succeeds. Later calls will see
        # the process die.
        def _kill():
            time.sleep(0.5)
            os._exit(1)
        threading.Thread(target=_kill, daemon=True).start()
        return "kill scheduled"


def classify(fn_name, call):
    """Run ``call`` and print (elapsed, exception class, first line of msg)."""
    start = time.time()
    try:
        r = call()
        print(f"  {fn_name}: ok in {time.time() - start:.3f}s -> {r!r}")
    except ActorDiedError as e:
        print(f"  {fn_name}: ActorDiedError in {time.time() - start:.3f}s")
        print(f"    msg[:200]: {str(e)[:200]}")
    except ActorUnavailableError as e:
        print(f"  {fn_name}: ActorUnavailableError in {time.time() - start:.3f}s")
        print(f"    msg[:200]: {str(e)[:200]}")
    except RayTaskError as e:
        cause = type(e.cause).__name__ if getattr(e, "cause", None) else None
        print(f"  {fn_name}: RayTaskError in {time.time() - start:.3f}s "
              f"(cause={cause})")
        print(f"    msg[:200]: {str(e)[:200]}")
    except GetTimeoutError:
        print(f"  {fn_name}: GetTimeoutError in {time.time() - start:.3f}s")
    except Exception as e:
        print(f"  {fn_name}: {type(e).__name__} in {time.time() - start:.3f}s")
        print(f"    msg[:200]: {str(e)[:200]}")


# ============================================================
# 1. Python-level exception (does actor process survive?)
# ============================================================
print("\n===== Test 1: RuntimeError in method =====")
p = Probe.remote()
classify("hello (pre)", lambda: ray.get(p.hello.remote(), timeout=5))
classify("raise_runtime", lambda: ray.get(p.raise_runtime.remote(), timeout=5))
classify("hello (post)", lambda: ray.get(p.hello.remote(), timeout=5))

print("\n===== Test 2: MemoryError in method =====")
classify("raise_memory", lambda: ray.get(p.raise_memory.remote(), timeout=5))
classify("hello (post-mem)", lambda: ray.get(p.hello.remote(), timeout=5))


# ============================================================
# 3. os._exit(1) inside a method (in-flight caller)
# ============================================================
print("\n===== Test 3: os._exit(1) inside method (in-flight) =====")
p2 = Probe.remote()
classify("hello (pre)", lambda: ray.get(p2.hello.remote(), timeout=5))
# The suicide method never returns; observe what the in-flight caller gets.
classify("suicide_exit (in-flight)",
         lambda: ray.get(p2.suicide_exit.remote(), timeout=10))

# Immediately follow-up: Ray should be restarting the actor.
print("  ... immediately after suicide ...")
classify("hello (during restart)",
         lambda: ray.get(p2.hello.remote(), timeout=10))

# Wait for restart to settle and confirm actor is back.
time.sleep(3)
classify("hello (after settle)",
         lambda: ray.get(p2.hello.remote(), timeout=10))


# ============================================================
# 4. Background thread inside actor calls os._exit(1)
# ============================================================
print("\n===== Test 4: background thread exits process =====")
p3 = Probe.remote()
classify("hello (pre)", lambda: ray.get(p3.hello.remote(), timeout=5))
# This call returns normally, then the thread exits the process 0.5s later.
classify("suicide_from_thread",
         lambda: ray.get(p3.suicide_from_thread.remote(), timeout=5))
time.sleep(1)  # let the thread's os._exit fire
classify("hello (right after)",
         lambda: ray.get(p3.hello.remote(), timeout=10))
time.sleep(3)
classify("hello (after settle)",
         lambda: ray.get(p3.hello.remote(), timeout=10))


print("\n===== done =====")