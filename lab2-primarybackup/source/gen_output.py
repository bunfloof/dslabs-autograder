#!/usr/bin/env python3
import re
import json
import sys
def parse_test_output(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    test_results = []
    total_score = 0
    current_test = None

    for line in lines:
	#This line is specific to lab and test.  The following is for lab 2, part 2.
        #if 'TEST' in line and ('[RUN]' in line or '[SEARCH]' in line):
        if 'TEST' in line:
            if current_test:
                # If the current test doesn't have a result, mark it as TIMEOUT
                current_test['score'] = 0
                current_test['output'] = 'TIMEOUT'
                test_results.append(current_test)

            test_number = line.split(':')[0].split(' ')[1].strip()
            test_name = line.split(':')[1].split('(')[0].strip()
            points_info = re.search(r'\((.*?)\)', line).group(1)
            max_points = float(points_info.split('pts')[0].strip())

            current_test = {
                'number': test_number,
                'name': test_name,
                'max_score': max_points,
                'output': 'RUNNING'
            }

        elif '...PASS' in line or '...FAIL' in line:
            if current_test:
                result = 'PASS' if '...PASS' in line else 'FAIL'
                score = max_points if result == 'PASS' else 0
                total_score += score

                current_test['score'] = score
                current_test['output'] = result
                test_results.append(current_test)
                current_test = None

    # Final check for the last test (like TEST 2.16)
    if current_test:
        current_test['score'] = 0
        current_test['output'] = 'TIMEOUT'
        test_results.append(current_test)

    # Sorting the tests by their number
    test_results.sort(key=lambda x: [int(num) for num in x['number'].split('.')])

    # Compile the final JSON object
    gradescope_result = {
        'score': total_score,
        'execution_time': None,
        'tests': test_results
    }

    return json.dumps(gradescope_result, indent=4)

# Replace 'file_path' with the path to the actual test output file
result_json = parse_test_output(sys.argv[1])
print(result_json)




