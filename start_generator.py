#!/usr/bin/env python3
"""
Quick Start Launcher for Data Generator
========================================
This script checks prerequisites and launches the data generator.
"""

import asyncio

try:
    asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

import sys
import subprocess
import os


def check_python_version():
    """Check if Python version is adequate"""
    if sys.version_info < (3, 7):
        print("✗ Python 3.7 or higher is required")
        print(f"  Current version: {sys.version}")
        return False
    print(f"✓ Python version: {sys.version_info.major}.{sys.version_info.minor}")
    return True


def check_dependencies():
    """Check if required packages are installed"""
    required = {
        'ib_insync': 'ib_insync>=0.9.86',
        'pandas': 'pandas>=1.5.0',
        'numpy': 'numpy>=1.23.0',
        'pytz': 'pytz>=2023.3'
    }
    
    missing = []
    for package, requirement in required.items():
        try:
            __import__(package)
            print(f"✓ {package} installed")
        except ImportError:
            print(f"✗ {package} not found")
            missing.append(requirement)
    
    if missing:
        print("\nMissing dependencies. Install with:")
        print(f"  pip install {' '.join(missing)}")
        print("\nOr use requirements.txt:")
        print("  pip install -r requirements.txt")
        return False
    
    return True


def check_ib_connection():
    """Check if IB Gateway/TWS might be running"""
    print("\n⚠ Important: Make sure Interactive Brokers Gateway or TWS is running")
    print("  and API connections are enabled in settings.")
    
    response = input("\nIs IB Gateway/TWS running with API enabled? (y/n): ").lower()
    return response == 'y'


def check_directories():
    """Create necessary directories"""
    dirs = ['Bruteforce_data']
    for directory in dirs:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✓ Created directory: {directory}")
        else:
            print(f"✓ Directory exists: {directory}")
    return True


def main():
    """Main launcher"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║           Data Generator Quick Start Launcher               ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    print("Checking prerequisites...\n")
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check directories
    if not check_directories():
        sys.exit(1)
    
    # Check IB connection readiness
    if not check_ib_connection():
        print("\nPlease start IB Gateway/TWS and try again.")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("✓ All checks passed! Launching data generator...")
    print("="*60 + "\n")
    
    # Launch the main script
    try:
        from data_generator import interactive_mode
        interactive_mode()
    except Exception as e:
        print(f"\n✗ Error launching data generator: {e}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ Launcher cancelled by user")
        sys.exit(0)