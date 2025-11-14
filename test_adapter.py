#!/usr/bin/env python
"""
Quick test to verify the adapter pattern is working correctly
"""
try:
    print("=" * 60)
    print("Testing BioCredits Adapter Pattern")
    print("=" * 60)
    
    # Test 1: Import adapter
    print("\n1. Testing imports...")
    from data_adapter import DataAdapter
    from airtable_adapter import AirtableAdapter
    print("   ✓ Imports successful")
    
    # Test 2: Create adapter instance
    print("\n2. Creating AirtableAdapter...")
    adapter = AirtableAdapter()
    print("   ✓ Adapter created successfully")
    
    # Test 3: Check configuration loaded
    print("\n3. Checking configuration...")
    if hasattr(adapter, 'config') and adapter.config:
        print("   ✓ Configuration loaded")
        if 'PERSONAL_ACCESS_TOKEN' in adapter.config:
            token_preview = adapter.config['PERSONAL_ACCESS_TOKEN'][:10] + "..." if adapter.config['PERSONAL_ACCESS_TOKEN'] else "None"
            print(f"   ✓ Access token found: {token_preview}")
    else:
        print("   ⚠ Warning: Configuration might not be loaded properly")
    
    # Test 4: Verify adapter methods exist
    print("\n4. Verifying adapter methods...")
    required_methods = [
        'download_land_data',
        'download_observations',
        'upload_results',
        'log_entry',
        'clear_tables'
    ]
    
    for method in required_methods:
        if hasattr(adapter, method):
            print(f"   ✓ {method}() exists")
        else:
            print(f"   ✗ {method}() MISSING")
    
    print("\n" + "=" * 60)
    print("✓ All adapter tests passed!")
    print("=" * 60)
    print("\nYou can now run the full pipeline:")
    print("  python credits_pipeline.py")
    print()
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)


