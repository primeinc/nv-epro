#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path

def test_notebook(notebook_path):
    """Test if a notebook can execute successfully"""
    cmd = [
        'jupyter', 'nbconvert', 
        '--to', 'notebook', 
        '--execute', 
        str(notebook_path),
        '--output-dir=output'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"[PASS] {notebook_path.name}")
            return True
        else:
            print(f"[FAIL] {notebook_path.name}")
            print(f"Error: {result.stderr[:200]}...")
            return False
    except Exception as e:
        print(f"[ERROR] {notebook_path.name}: {e}")
        return False

def main():
    notebooks = [
        Path('notebooks/01-vendor-concentration.ipynb'),
        Path('notebooks/02-bid-competition.ipynb'), 
        Path('notebooks/03-anomaly-detection.ipynb'),
        Path('notebooks/04-procurement-cycles.ipynb'),
        Path('notebooks/05-executive-summary.ipynb')
    ]
    
    print("Testing all analytics notebooks...")
    
    results = {}
    for notebook in notebooks:
        if notebook.exists():
            results[notebook.name] = test_notebook(notebook)
        else:
            print(f"[MISSING] {notebook.name}")
            results[notebook.name] = False
    
    print(f"\nTest Summary:")
    passed = sum(results.values())
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("All notebooks are working!")
    else:
        print("Some notebooks need fixes")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)