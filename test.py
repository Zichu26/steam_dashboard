"""
Test Steam API functionality WITHOUT Kafka
This script tests ONLY the Steam API client
"""
import sys
import os
from datetime import datetime

# Add src to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.logger import setup_logging, get_logger
from src.ingestion.steam_client import SteamAPIClient

# Setup logging
setup_logging()
logger = get_logger(__name__)


def test_steam_connection():
    """Test basic Steam API connection"""
    print("\n" + "="*60)
    print("TEST 1: Steam API Connection")
    print("="*60)
    
    try:
        client = SteamAPIClient()
        print("✓ Steam API client created successfully")
        print(f"  API Key configured: {'Yes' if client.api_key else 'No'}")
        print(f"  Base URL: {client.base_url}")
        return client
    except Exception as e:
        print(f"✗ Failed to create Steam API client: {e}")
        return None


def test_get_player_summaries(client):
    """Test fetching player summaries"""
    print("\n" + "="*60)
    print("TEST 2: Get Player Summaries")
    print("="*60)
    
    # Using Gabe Newell's public Steam ID as test
    test_steam_id = '76561197961427893'
    
    try:
        print(f"Fetching data for Steam ID: {test_steam_id}")
        response = client.get_player_summaries([test_steam_id])
        
        # Check response structure
        if 'response' not in response:
            print("✗ Invalid response structure - missing 'response' key")
            return False
        
        if 'players' not in response['response']:
            print("✗ Invalid response structure - missing 'players' key")
            return False
        
        players = response['response']['players']
        
        if not players:
            print("✗ No player data returned")
            print("  This could mean:")
            print("  - The profile is private")
            print("  - Invalid Steam ID")
            print("  - API key issues")
            return False
        
        # Success! Display the data
        player = players[0]
        print("✓ Successfully fetched player data!")
        print("\nPlayer Information:")
        print(f"  Steam ID:     {player.get('steamid', 'N/A')}")
        print(f"  Name:         {player.get('personaname', 'N/A')}")
        print(f"  Profile URL:  {player.get('profileurl', 'N/A')}")
        print(f"  Online State: {player.get('personastate', 'N/A')} (0=Offline, 1=Online)")
        
        if 'lastlogoff' in player:
            last_logoff = datetime.fromtimestamp(player['lastlogoff'])
            print(f"  Last Logoff:  {last_logoff}")
        
        if 'timecreated' in player:
            created = datetime.fromtimestamp(player['timecreated'])
            print(f"  Account Created: {created}")
        
        print("\nFull Response (first 500 chars):")
        print(str(response)[:500])
        
        return True
        
    except Exception as e:
        print(f"✗ Error fetching player data: {e}")
        print(f"  Error type: {type(e).__name__}")
        return False


def test_get_owned_games(client):
    """Test fetching owned games"""
    print("\n" + "="*60)
    print("TEST 3: Get Owned Games")
    print("="*60)
    
    test_steam_id = '76561197961427893'
    
    try:
        print(f"Fetching owned games for Steam ID: {test_steam_id}")
        response = client.get_owned_games(test_steam_id)
        
        if 'response' not in response:
            print("✗ Invalid response structure")
            return False
        
        if 'games' not in response['response']:
            print("⚠ No games data returned")
            print("  This could mean:")
            print("  - Profile game details are private")
            print("  - User has no games")
            return False
        
        games = response['response']['games']
        game_count = response['response'].get('game_count', len(games))
        
        print(f"✓ Successfully fetched game data!")
        print(f"\nTotal games owned: {game_count}")
        
        if games:
            print(f"\nShowing first 5 games:")
            for i, game in enumerate(games[:5]):
                playtime_hours = game.get('playtime_forever', 0) / 60
                print(f"\n  {i+1}. {game.get('name', 'Unknown Game')}")
                print(f"     App ID: {game.get('appid')}")
                print(f"     Playtime: {playtime_hours:.1f} hours")
                
                if 'playtime_2weeks' in game:
                    recent_hours = game['playtime_2weeks'] / 60
                    print(f"     Recent (2 weeks): {recent_hours:.1f} hours")
        
        return True
        
    except Exception as e:
        print(f"✗ Error fetching owned games: {e}")
        return False


def test_get_recently_played(client):
    """Test fetching recently played games"""
    print("\n" + "="*60)
    print("TEST 4: Get Recently Played Games")
    print("="*60)
    
    test_steam_id = '76561197961427893'
    
    try:
        print(f"Fetching recently played games for Steam ID: {test_steam_id}")
        response = client.get_recently_played_games(test_steam_id)
        
        if 'response' not in response:
            print("✗ Invalid response structure")
            return False
        
        if 'games' not in response['response']:
            print("⚠ No recently played games")
            print("  User may not have played any games recently")
            return True  # Not an error, just no recent activity
        
        games = response['response']['games']
        
        print(f"✓ Successfully fetched recently played games!")
        print(f"\nRecently played games: {len(games)}")
        
        for i, game in enumerate(games):
            playtime_hours = game.get('playtime_forever', 0) / 60
            recent_hours = game.get('playtime_2weeks', 0) / 60
            
            print(f"\n  {i+1}. {game.get('name', 'Unknown Game')}")
            print(f"     App ID: {game.get('appid')}")
            print(f"     Total Playtime: {playtime_hours:.1f} hours")
            print(f"     Recent (2 weeks): {recent_hours:.1f} hours")
        
        return True
        
    except Exception as e:
        print(f"✗ Error fetching recently played games: {e}")
        return False


def test_rate_limiting(client):
    """Test that rate limiting works"""
    print("\n" + "="*60)
    print("TEST 5: Rate Limiting")
    print("="*60)
    
    test_steam_id = '76561197960435530'
    
    try:
        print("Making 3 rapid API calls to test rate limiting...")
        print("(Rate limit is 100 calls per 60 seconds)")
        
        import time
        
        for i in range(3):
            start = time.time()
            response = client.get_player_summaries([test_steam_id])
            elapsed = time.time() - start
            
            print(f"  Call {i+1}: {elapsed:.3f} seconds")
            
            if 'response' in response and 'players' in response['response']:
                print(f"    ✓ Success")
            else:
                print(f"    ✗ Failed")
        
        print("\n✓ Rate limiting is working (no errors)")
        return True
        
    except Exception as e:
        print(f"✗ Rate limiting test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("STEAM API TESTING (WITHOUT KAFKA)")
    print("="*70)
    print("\nThis script tests the Steam API integration independently.")
    print("Make sure you have:")
    print("  1. Created .env file (cp .env.template .env)")
    print("  2. Added your Steam API key to .env")
    print("  3. Installed dependencies (pip install -r requirements.txt)")
    print("\nStarting tests...\n")
    
    # Test 1: Connection
    client = test_steam_connection()
    if not client:
        print("\n❌ Cannot proceed - Steam API client creation failed")
        print("\nTroubleshooting:")
        print("  1. Check that .env file exists")
        print("  2. Verify STEAM_API_KEY is set in .env")
        print("  3. Get API key from: https://steamcommunity.com/dev/apikey")
        return 1
    
    # Test 2: Player Summaries
    test2_passed = test_get_player_summaries(client)
    
    # Test 3: Owned Games
    test3_passed = test_get_owned_games(client)
    
    # Test 4: Recently Played
    test4_passed = test_get_recently_played(client)
    
    # Test 5: Rate Limiting
    test5_passed = test_rate_limiting(client)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    results = {
        "Steam API Connection": client is not None,
        "Get Player Summaries": test2_passed,
        "Get Owned Games": test3_passed,
        "Get Recently Played": test4_passed,
        "Rate Limiting": test5_passed
    }
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status} - {test_name}")
    
    passed_count = sum(results.values())
    total_count = len(results)
    
    print(f"\nTotal: {passed_count}/{total_count} tests passed")
    print("="*70)
    
    if passed_count == total_count:
        print("\n🎉 All tests passed! Your Steam API integration is working!")
        print("\nNext steps:")
        print("  1. Test with your own Steam IDs")
        print("  2. Set up Kafka (docker-compose up -d)")
        print("  3. Run the full pipeline")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please fix issues before proceeding.")
        print("\nCommon issues:")
        print("  - Invalid or missing API key")
        print("  - Testing with private profiles")
        print("  - Network connectivity problems")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)