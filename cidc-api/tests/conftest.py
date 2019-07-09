import os
import sys

# Add cidc-api modules to path
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(test_dir, ".."))

os.environ["TESTING"] = "True"
