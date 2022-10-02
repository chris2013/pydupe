import time
import subprocess
start_time = time.time()

cmd= ['pydupe','dd', '/home/chris/Pictures/']
sub = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr = subprocess.PIPE)
stdout, stderr = sub.communicate()

print("--- %s seconds ---" % (time.time() - start_time))

# HASH:
# ~/Pictures/ :
# SET: --- 4.48984169960022 seconds ---
# DEQUE: --- 4.521728277206421 seconds ---

# ~/
# SET: --- 134.3529691696167 seconds - --
# DEQUE: --- 136.01621294021606 seconds ---

# ~/Pictures/ with 5 copies:
# SET: --- 14.386288166046143 seconds ---
# DEQUE: --- 14.834105730056763 seconds ---

# DEDUPE:
# ~/Pictures/ with 5 copies:
# SET: --- 0.40973687171936035 seconds ---
# DEQUE:--- 0.4015944004058838 seconds --- 

