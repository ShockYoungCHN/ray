import ray
import numpy as np
import time
import os

# Initialize Ray with a small object store to trigger spilling quickly
# and set the spilling threshold to a very low value for testing.
ray.init(
    object_store_memory=100 * 1024 * 1024,  # 100 MB
    _system_config={
        "object_spilling_threshold": 0.1,  # Spill at 10% usage
    }
)

print("Ray initialized.")
print(f"Object store memory: {ray.cluster_resources().get('object_store_memory', 0) / 1024 / 1024:.2f} MB")

# Create large objects to fill the store
objects = []
try:
    for i in range(20):
        # Each object is ~10MB
        data = np.random.bytes(10 * 1024 * 1024)
        obj_ref = ray.put(data)
        objects.append(obj_ref)
        print(f"Put object {i+1}, total size ~{(i+1)*10} MB")
        
        # Give it a moment to trigger background spilling logic
        time.sleep(0.5)
        
    print("Wait for 5 seconds to let spilling complete...")
    time.sleep(5)
    
    # Verify we can still get the objects (they should be restored from spill)
    for i, ref in enumerate(objects):
        _ = ray.get(ref)
        if i % 5 == 0:
            print(f"Verified object {i}")

finally:
    ray.shutdown()
