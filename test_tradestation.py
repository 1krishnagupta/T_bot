#!/usr/bin/env python3
"""
TradeStation Authentication Test Script
This script tests different authentication methods for TradeStation API
"""

import requests
import os
import json
import base64
from datetime import datetime

# TradeStation Credentials
CLIENT_ID = "6ZhZile2KIwtU2xwdNGBYdNpPmRynB5J"
CLIENT_SECRET = "tY4dNJuhFst_XeqMmB95pF2_EriSqxc-ruQdnNILc4L5_vm9M0Iixwf9FUGw-WbQ"

# Check for refresh token in environment
REFRESH_TOKEN = os.environ.get('TRADESTATION_REFRESH_TOKEN', '')

print("=" * 80)
print("TradeStation Authentication Test")
print("=" * 80)
print(f"Client ID: {CLIENT_ID[:10]}...")
print(f"Client Secret: {CLIENT_SECRET[:10]}...")
print(f"Refresh Token: {'Found' if REFRESH_TOKEN else 'NOT FOUND'}")
print("=" * 80)

def test_refresh_token_auth():
    """Test authentication using refresh token (like the reference code)"""
    print("\n[1] Testing Refresh Token Authentication...")
    
    if not REFRESH_TOKEN:
        print("   ❌ No refresh token found!")
        print("   Set environment variable: TRADESTATION_REFRESH_TOKEN=your_token")
        return None
    
    url = "https://signin.tradestation.com/oauth/token"
    
    payload = f'grant_type=refresh_token&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&refresh_token={REFRESH_TOKEN}'
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    try:
        print(f"   Making request to: {url}")
        response = requests.post(url, headers=headers, data=payload)
        print(f"   Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            access_token = data.get('access_token')
            print(f"   ✅ Success! Access Token: {access_token[:20]}...")
            return access_token
        else:
            print(f"   ❌ Failed: {response.text}")
            return None
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return None

def test_client_credentials():
    """Test client credentials flow (might not work for TradeStation)"""
    print("\n[2] Testing Client Credentials Flow...")
    
    url = "https://signin.tradestation.com/oauth/token"
    
    # Create basic auth header
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        "Authorization": f"Basic {auth_b64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "grant_type": "client_credentials",
        "scope": "marketdata"
    }
    
    try:
        print(f"   Making request to: {url}")
        response = requests.post(url, headers=headers, data=data)
        print(f"   Response Status: {response.status_code}")
        
        if response.status_code == 200:
            token_data = response.json()
            print(token_data)
            print(f"   ✅ Success! Token: {token_data}")
            return token_data.get('access_token')
        else:
            print(f"   ❌ Failed: {response.text}")
            return None
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return None

def test_api_endpoints():
    """Test different API endpoints to see which work"""
    print("\n[3] Testing API Endpoints...")
    
    endpoints = [
        "https://signin.tradestation.com/oauth/token",
        "https://signin.tradestation.com/authorize",
        "https://api.tradestation.com/v2/security/authorize",
        "https://api.cert.tradestation.com/v2/security/authorize"
    ]
    
    for endpoint in endpoints:
        try:
            print(f"\n   Testing: {endpoint}")
            response = requests.get(endpoint, timeout=5)
            print(f"   Status: {response.status_code}")
            if response.status_code < 500:
                print(f"   ✅ Endpoint is reachable")
            else:
                print(f"   ⚠️  Server error")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

def test_market_data_request(access_token):
    """Test if we can fetch market data with the token"""
    print("\n[4] Testing Market Data Request...")
    
    if not access_token:
        print("   ❌ No access token to test with")
        return
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    # Test fetching SPY quote
    url = "https://api.tradestation.com/v3/marketdata/quotes/SPY"
    
    try:
        print(f"   Fetching quote for SPY...")
        response = requests.get(url, headers=headers)
        print(f"   Response Status: {response.status_code}")
        
        if response.status_code == 200:
            print(f"   ✅ Success! Market data accessible")
            print(f"   Data: {response.json()}")
        else:
            print(f"   ❌ Failed: {response.text}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")

def get_oauth_instructions():
    """Print instructions for getting a refresh token"""
    print("\n[5] OAuth2 Authorization Instructions")
    print("=" * 80)
    print("To get a refresh token, you need to:")
    print()
    print("1. Build the authorization URL:")
    
    auth_url = f"https://signin.tradestation.com/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=http://localhost:8080&audience=https://api.tradestation.com&scope=openid profile MarketData ReadAccount Trade"
    
    print(f"\n   {auth_url}")
    print()
    print("2. Visit this URL in your browser")
    print("3. Log in with your TradeStation credentials")
    print("4. Authorize the application")
    print("5. You'll be redirected to: http://localhost:8080?code=AUTHORIZATION_CODE")
    print("6. Copy the AUTHORIZATION_CODE")
    print()
    print("7. Exchange the code for tokens:")
    print()
    print("   POST https://signin.tradestation.com/oauth/token")
    print("   Content-Type: application/x-www-form-urlencoded")
    print()
    print("   Body:")
    print(f"   grant_type=authorization_code")
    print(f"   code=YOUR_AUTHORIZATION_CODE")
    print(f"   client_id={CLIENT_ID}")
    print(f"   client_secret={CLIENT_SECRET}")
    print(f"   redirect_uri=http://localhost:8080")
    print()
    print("8. The response will contain your refresh_token")
    print("9. Save it as environment variable:")
    print("   export TRADESTATION_REFRESH_TOKEN=your_refresh_token")

def main():
    """Run all tests"""
    
    # Test 1: Try refresh token auth (like reference code)
    access_token = test_refresh_token_auth()
    
    # Test 2: Try client credentials (might not work)
    if not access_token:
        access_token = test_client_credentials()
    
    # Test 3: Check API endpoints
    test_api_endpoints()
    
    # Test 4: Try market data if we got a token
    if access_token:
        test_market_data_request(access_token)
    
    # Show OAuth instructions
    if not REFRESH_TOKEN:
        get_oauth_instructions()
    
    print("\n" + "=" * 80)
    print("Test Complete")
    print("=" * 80)
    
    if access_token:
        print("✅ Authentication successful!")
        print(f"Access Token: {access_token[:30]}...")
    else:
        print("❌ Authentication failed!")
        print("\nNext steps:")
        if not REFRESH_TOKEN:
            print("1. You need to get a refresh token through OAuth2 flow")
            print("2. Follow the instructions above")
            print("3. Set the TRADESTATION_REFRESH_TOKEN environment variable")
            print("4. Run this test again")

if __name__ == "__main__":
    main()