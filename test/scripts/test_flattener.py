import kafkaopmon
import json
import sys

# Check that an argument was provided
if len(sys.argv)<2:
  print("Syntax: python python-flattener <json_file>")
  quit()

# Open and read JSON file
file = sys.argv[1]
f = open(file)

j = json.load(f)

print(j)

print("------")

# Flatten using bound function
iqb = kafkaopmon.JsonFlattener(j)
f = iqb.get()

for field in f:
   print(field)


