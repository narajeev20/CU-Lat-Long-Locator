import time
import random
import Quartz

start_time = time.time()
run_for_seconds = 5 * 60 * 6  # 5 minutes

while time.time() - start_time < run_for_seconds:
    x = random.randint(200, 400)
    y = random.randint(200, 400)
    Quartz.CGWarpMouseCursorPosition((x, y))
    time.sleep(10)