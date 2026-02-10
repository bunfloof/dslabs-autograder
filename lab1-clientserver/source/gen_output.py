#!/usr/bin/env python3
import json
import sys
import os

with open(sys.argv[1], 'r') as fh:
	while True:
		line = fh.readline()
		if not line:
			break
		if line.startswith('Tests passed'):
			line = line.strip()
			points = fh.readline().strip()
			timespent = fh.readline().strip()
			score = points.split(':')[1]
			value = score.split("/")[0].strip()
			value = int(value) / 2
			score = "score: " + str(value)
			test_str = score + ", " + line + ", " + timespent
			res = test_str.split(', ')
			res = {i.split(':')[0]: i.split(': ')[1] for i in res}
			res["output"] = "Text relevant to the entire submission"
			res["stdout_visibility"] = "visible"
			res["output_format"] = "simple_format"
			print(json.dumps(res, indent=4))



