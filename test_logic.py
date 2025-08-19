#!/usr/bin/env python3
"""
Test script to demonstrate the new smart scraping logic
"""

import asyncio
from main import OptimizedCNPJScraper

async def test_logic():
    """Test the new smart scraping logic"""
    print("🧪 Testing New Smart Scraping Logic")
    print("=" * 50)
    
    scraper = OptimizedCNPJScraper()
    
    # Test CNPJs with different scenarios
    test_cnpjs = [
        "07134405000161",  # Should have some API data
        "25962788000100",  # Test CNPJ
        "00000000000191",  # Petrobras (should have API data)
    ]
    
    for cnpj in test_cnpjs:
        print(f"\n📋 Testing CNPJ: {cnpj}")
        print("-" * 30)
        
        try:
            # Create session for API calls
            await scraper.create_session()
            
            # Test the scraping logic
            result = await scraper.scrape_cnpj(cnpj)
            
            print(f"✅ Result for {cnpj}:")
            print(f"  Company: {result.nome_empresa}")
            print(f"  Phone: {result.telefone}")
            print(f"  Email: {result.email}")
            print(f"  Source: {result.source}")
            
        except Exception as e:
            print(f"❌ Error testing {cnpj}: {str(e)}")
        
        finally:
            # Close session
            await scraper.close_session()
    
    print("\n🎯 Test completed!")

async def test_granular_scraping():
    """Test the new granular web scraping approach"""
    print("\n🔍 Testing Granular Web Scraping")
    print("=" * 50)
    
    scraper = OptimizedCNPJScraper()
    
    # Test CNPJ that likely needs web scraping
    test_cnpj = "07134405000161"
    
    print(f"📋 Testing granular scraping for CNPJ: {test_cnpj}")
    print("-" * 40)
    
    try:
        # Test phone-only scraping
        print("📞 Testing phone-only web scraping...")
        phone_data = await scraper.scrape_additional_info(test_cnpj, look_for_phone=True, look_for_email=False)
        if phone_data:
            print(f"  Phone found: {phone_data.get('telefone', 'Not found')}")
        else:
            print("  No phone data found")
        
        # Test email-only scraping
        print("📧 Testing email-only web scraping...")
        email_data = await scraper.scrape_additional_info(test_cnpj, look_for_phone=False, look_for_email=True)
        if email_data:
            print(f"  Email found: {email_data.get('email', 'Not found')}")
        else:
            print("  No email data found")
        
        # Test both
        print("📞📧 Testing both phone and email scraping...")
        both_data = await scraper.scrape_additional_info(test_cnpj, look_for_phone=True, look_for_email=True)
        if both_data:
            print(f"  Phone: {both_data.get('telefone', 'Not found')}")
            print(f"  Email: {both_data.get('email', 'Not found')}")
        else:
            print("  No data found")
            
    except Exception as e:
        print(f"❌ Error testing granular scraping: {str(e)}")
    
    print("\n🎯 Granular scraping test completed!")

if __name__ == "__main__":
    asyncio.run(test_logic())
    asyncio.run(test_granular_scraping()) 