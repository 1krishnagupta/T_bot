import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Code.bot_core.tradestation_api import TradeStationAPI

def main():
    print("TradeStation API Credential Helper")
    print("="*50)
    print("\nTo get your API credentials:")
    print("1. Go to https://developer.tradestation.com/")
    print("2. Log in with your TradeStation account")
    print("3. Go to 'My Apps' section")
    print("4. Create a new application if you haven't already")
    print("5. Copy your API Key and Secret")
    print("\nThen update your credentials.txt file with these values.")
    
    # Try to login with existing credentials
    try:
        api = TradeStationAPI()
        if api.login():
            print("\n[✓] Successfully logged in!")
            
            # Get account IDs
            if hasattr(api, 'userid') and api.userid:
                endpoint = f"/v2/users/{api.userid}/accounts"
                response = api.safe_request("GET", endpoint)
                if response.status_code == 200:
                    accounts = response.json()
                    print(f"\n[✓] Found {len(accounts)} account(s):")
                    for acc in accounts:
                        print(f"    - Account ID: {acc.get('Key')}")
                        print(f"      Name: {acc.get('Name')}")
                        print(f"      Type: {acc.get('TypeDescription')}")
                        print(f"      Status: {acc.get('StatusDescription')}")
        else:
            print("\n[✗] Login failed. Check your credentials.")
    except Exception as e:
        print(f"\n[✗] Error: {e}")

if __name__ == "__main__":
    main()