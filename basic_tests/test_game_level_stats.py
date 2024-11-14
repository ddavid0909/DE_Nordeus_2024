import unittest
import sys
import importlib

"""
Disclaimer:
The tests given won't be used when grading the project, they are there to help you. 
These tests cover only edge cases that are found in events_test.jsonl, which is only a portion of all edge cases found in events.jsonl.
If you are not writing in python, this file can still help you as it explains events_test.jsonl and has a solution for one input (line 35).
You can completely ignore these tests if you want, no points will be deducteed.

Explanation of sample dataset:
It contains 3 registrations, 5 sessions and 1 match all in 1 day.

How to run tests:
python test_game_level_stats.py name_of_your_python_file name_of_your_function
Function should take one parameter; date
Python 3.8+ recommended
"""

class TestGameLevelStats(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if len(sys.argv) < 3:
            print("Usage: python test_game_level_stats.py <module_name> <function_name>")
            sys.exit(1)

        module_name = sys.argv[1]
        function_name = sys.argv[2]

        module = importlib.import_module(module_name)
        cls.function_to_test = getattr(module, function_name)

        cls.expected_result = {
            "active_users": 3,
            "number_of_sessions": 5,
            "avg_sessions_for_users": 5/3,
            "user_with_most_points": "52d65a1b-8012-934e-001b-19e6ba3cdc0e"
        }
        
        cls.result = cls.function_to_test(date=None)

    def test_dau(self):
        self.assertEqual(self.result['active_users'], self.expected_result['active_users'], "Active users should be 3")

    def test_total_sessions(self):
        self.assertEqual(self.result['number_of_sessions'], self.expected_result['number_of_sessions'], "Total number of sessions should be 5")

    def test_avg_sessions_per_user(self):
        self.assertAlmostEqual(self.result['avg_sessions_for_users'], self.expected_result['avg_sessions_for_users'], places=2, msg="Average sessions per user should be 5/3")

    def test_user_with_most_points(self):
        self.assertEqual(self.result['user_with_most_points'], self.expected_result['user_with_most_points'], "User with most points should be '52d65a1b-8012-934e-001b-19e6ba3cdc0e'")

if __name__ == '__main__':
    unittest.main(argv=sys.argv[:1])
