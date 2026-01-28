"""
Test script for verifying all connections in the project
"""

import sys
import os

def test_postgresql():
    """Test PostgreSQL connection"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="",
            port=5432
        )
        conn.close()
        print("✅ PostgreSQL connection: SUCCESS")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL connection: FAILED - {e}")
        return False

def test_python_dependencies():
    """Test Python dependencies"""
    dependencies = ['pandas', 'numpy', 'sqlalchemy', 'psycopg2']
    
    for dep in dependencies:
        try:
            __import__(dep)
            print(f"✅ {dep}: SUCCESS")
        except ImportError:
            print(f"❌ {dep}: FAILED")
            return False
    
    return True

def test_airflow():
    """Check if Airflow is accessible"""
    try:
        import airflow
        print(f"✅ Apache Airflow: Version {airflow.__version__}")
        return True
    except ImportError:
        print("❌ Apache Airflow: NOT INSTALLED")
        return False

if __name__ == "__main__":
    print("🔍 Spotify Analytics - Connection Tests")
    print("=" * 50)
    
    tests = [
        ("Python Dependencies", test_python_dependencies),
        ("PostgreSQL", test_postgresql),
        ("Apache Airflow", test_airflow),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 Testing: {test_name}")
        results.append(test_func())
    
    print("\n" + "=" * 50)
    if all(results):
        print("🎉 ALL TESTS PASSED! Project is ready.")
    else:
        print("⚠️ Some tests failed. Check your setup.")
        sys.exit(1)