import sys
import os

# Add the src directory to sys.path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.append(src_path)

# Print sys.path for verification
print(f"sys.path during pytest: {sys.path}")
