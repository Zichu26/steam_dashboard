"""
Steam Analytics Dashboard

A comprehensive dashboard displaying:
- Live Leaderboard of Games (streamed)
- Popular Games by Tag (batch)
- User Profile Summary (stream/micro-batch)
- Game Sentiment Generator (Airflow batch)
- Recommend Me a Game (batch)
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import os
import time
from openai import OpenAI

# Initialize OpenAI client
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Page config
st.set_page_config(
    page_title="Steam Analytics Dashboard",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        color: #1e3a5f;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c5282;
        border-bottom: 2px solid #4299e1;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .stDataFrame {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)


# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT', 'NMB12256'),
        user=os.environ.get('SNOWFLAKE_USER', 'dog'),
        private_key_file=os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE', '/Users/aiko/.ssh/rsa_key.p8'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'STEAM_ANALYTICS'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'DOG_WH'),
        role=os.environ.get('SNOWFLAKE_ROLE', 'TRAINING_ROLE')
    )


@st.cache_data(ttl=30)  # Cache for 30 seconds for live data
def get_live_leaderboard(limit=50):
    """Get live game leaderboard from GAME_PLAYER_COUNTS_LIVE view (powered by player_count_producer)"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT
        game_id,
        game_name,
        current_players as player_count,
        avg_players_24h,
        avg_players_alltime,
        peak_players,
        trend,
        last_updated,
        data_points
    FROM GAME_PLAYER_COUNTS_LIVE
    WHERE game_name IS NOT NULL AND current_players IS NOT NULL
    ORDER BY current_players DESC NULLS LAST
    LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=60)
def get_games_by_tag(tag_filter=None, limit=20):
    """Get popular games filtered by tag"""
    conn = get_snowflake_connection()

    if tag_filter:
        query = f"""
        SELECT
            tag_name,
            game_name,
            appid as game_id,
            owners as total_downloads,
            price_cents,
            current_players
        FROM GAMES_BY_TAG
        WHERE tag_name = '{tag_filter}'
        ORDER BY current_players DESC NULLS LAST
        LIMIT {limit}
        """
    else:
        query = f"""
        SELECT DISTINCT tag_name
        FROM GAMES_BY_TAG
        ORDER BY tag_name
        LIMIT 100
        """

    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=60)
def get_available_tags():
    """Get list of available tags"""
    conn = get_snowflake_connection()
    query = """
    SELECT DISTINCT tag_name, COUNT(*) as game_count
    FROM GAMES_BY_TAG
    GROUP BY tag_name
    HAVING COUNT(*) >= 5
    ORDER BY game_count DESC
    LIMIT 50
    """
    df = pd.read_sql(query, conn)
    return df['TAG_NAME'].tolist()


@st.cache_data(ttl=300)
def get_user_profile_summary(steam_id=None):
    """Get user's game summary"""
    conn = get_snowflake_connection()

    if steam_id:
        query = f"""
        SELECT
            ps.steamid,
            p.personaname,
            ps.appid as game_id,
            ps.name as game_name,
            gi.genre as game_tag,
            ps.playtime_forever as hrs_played,
            ps.playtime_2weeks as recent_hrs,
            ps.rtime_last_played as last_played
        FROM PLAYER_STATS ps
        LEFT JOIN PLAYERS p ON ps.steamid = p.steamid
        LEFT JOIN GAME_INFO gi ON ps.appid = gi.appid
        WHERE ps.steamid = '{steam_id}'
        AND ps.name IS NOT NULL
        ORDER BY ps.playtime_forever DESC
        LIMIT 50
        """
    else:
        # Get sample users
        query = """
        SELECT DISTINCT steamid, personaname
        FROM PLAYERS
        WHERE personaname IS NOT NULL
        LIMIT 20
        """

    df = pd.read_sql(query, conn)
    return df


@st.cache_data(ttl=300)
def get_game_sentiments(limit=20):
    """Get game sentiment analysis results"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT
        appid as game_id,
        game_name,
        total_reviews_analyzed,
        ROUND(avg_sentiment_compound, 3) as avg_sentiment,
        ROUND(positive_review_pct, 1) as positive_pct,
        ROUND(negative_review_pct, 1) as negative_pct,
        ROUND(sentiment_score, 1) as sentiment_score,
        analysis_date
    FROM GAME_SENTIMENT_ANALYSIS
    WHERE analysis_date = (SELECT MAX(analysis_date) FROM GAME_SENTIMENT_ANALYSIS)
    ORDER BY sentiment_score DESC
    LIMIT {limit}
    """
    try:
        df = pd.read_sql(query, conn)
        return df
    except:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_game_reviews_sample(game_id, limit=10):
    """Get sample reviews for a game"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT
        game_name,
        review_text,
        voted_up,
        playtime_at_review,
        timestamp_created
    FROM GAME_REVIEWS
    WHERE appid = {game_id}
    ORDER BY timestamp_created DESC
    LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    return df


def get_game_recommendations_ai(game_name, game_description, available_games):
    """Use OpenAI to get smart game recommendations with explanations"""
    import json
    import re

    if not openai_client:
        st.warning("OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")
        return []

    try:
        games_list = "\n".join([f"- {g['name']} ({g['genres']}): {g['description'][:150]}..."
                                for g in available_games[:50]])

        prompt = f"""Based on this game the user likes:
Game: {game_name}
Description: {game_description[:500] if game_description else 'No description available'}

From this list of available games, recommend 5 games that are most similar in gameplay, genre, or theme:

{games_list}

Return ONLY a valid JSON object (no markdown, no explanation) with recommendations in this exact format:
{{"recommendations": [{{"name": "Exact Game Name", "reason": "Brief explanation (20-30 words)"}}]}}

IMPORTANT: Game names must match EXACTLY from the list above. Return only the JSON, nothing else.
"""

        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=600
        )

        content = response.choices[0].message.content.strip()

        # Remove markdown code blocks if present
        if "```json" in content:
            content = re.sub(r'```json\s*', '', content)
            content = re.sub(r'```\s*$', '', content)
        elif "```" in content:
            content = re.sub(r'```\s*', '', content)

        content = content.strip()

        result = json.loads(content)
        recommendations = result.get('recommendations', [])

        # Validate each recommendation has required fields
        valid_recs = []
        for rec in recommendations:
            if isinstance(rec, dict) and 'name' in rec:
                valid_recs.append({
                    'name': rec.get('name', ''),
                    'reason': rec.get('reason', 'Similar gameplay and genre.')
                })

        return valid_recs
    except json.JSONDecodeError as e:
        st.warning(f"AI response parsing error: {e}")
        return []
    except Exception as e:
        st.warning(f"AI recommendation error: {e}")
        return []

def get_game_recommendations(game_id):
    """Get game recommendations based on similar genres using GAME_DETAILS"""
    conn = get_snowflake_connection()

    # First get the source game info
    source_query = f"""
    SELECT gd.appid, gd.name, gd.genres, gd.short_description,
           gi.ccu, gi.positive, gi.negative
    FROM GAME_DETAILS gd
    LEFT JOIN GAME_INFO gi ON gd.appid = gi.appid
    WHERE gd.appid = {game_id}
    """
    source_df = pd.read_sql(source_query, conn)

    if source_df.empty:
        # Fallback: try to get from GAME_INFO only
        source_query = f"SELECT appid, name FROM GAME_INFO WHERE appid = {game_id}"
        source_df = pd.read_sql(source_query, conn)
        if source_df.empty:
            return pd.DataFrame(), []

    source_game = source_df.iloc[0]
    source_genres = source_game.get('GENRES', '[]') if 'GENRES' in source_df.columns else '[]'
    source_description = source_game.get('SHORT_DESCRIPTION', '') if 'SHORT_DESCRIPTION' in source_df.columns else ''

    # Get available games with details for AI recommendation
    available_query = """
    SELECT gd.appid, gd.name, gd.genres, gd.short_description,
           gi.ccu as player_count, gi.positive, gi.negative,
           ROUND(gi.positive * 100.0 / NULLIF(gi.positive + gi.negative, 0), 1) as review_score
    FROM GAME_DETAILS gd
    LEFT JOIN GAME_INFO gi ON gd.appid = gi.appid
    WHERE gd.short_description IS NOT NULL
    AND gi.ccu > 0
    ORDER BY gi.ccu DESC
    LIMIT 100
    """
    available_df = pd.read_sql(available_query, conn)

    if available_df.empty:
        return pd.DataFrame(), []

    # Filter out the source game
    available_df = available_df[available_df['APPID'] != game_id]

    # Prepare games for AI
    available_games = []
    for _, row in available_df.iterrows():
        available_games.append({
            'name': row['NAME'],
            'genres': row['GENRES'] if row['GENRES'] else '[]',
            'description': row['SHORT_DESCRIPTION'] if row['SHORT_DESCRIPTION'] else '',
            'appid': row['APPID'],
            'player_count': row['PLAYER_COUNT'],
            'review_score': row['REVIEW_SCORE']
        })

    # Get AI recommendations with reasons
    recommendations = get_game_recommendations_ai(
        source_game['NAME'],
        source_description,
        available_games
    )

    if recommendations:
        # Extract game names from recommendations
        recommended_names = [r.get('name', '') for r in recommendations]
        # Create a mapping of name to reason
        reasons_map = {r.get('name', ''): r.get('reason', 'Similar gameplay and genre.') for r in recommendations}

        # Filter to get recommended games with their stats
        result_df = available_df[available_df['NAME'].isin(recommended_names)].copy()
        if not result_df.empty:
            # Add reasons column
            result_df['REASON'] = result_df['NAME'].map(reasons_map)
            result_df = result_df.rename(columns={
                'APPID': 'appid',
                'NAME': 'name',
                'GENRES': 'genre',
                'PLAYER_COUNT': 'player_count',
                'REVIEW_SCORE': 'review_score',
                'REASON': 'reason'
            })
            return result_df[['appid', 'name', 'genre', 'player_count', 'review_score', 'reason']], recommendations

    # Fallback: genre-based matching using GAMES_BY_TAG
    fallback_query = f"""
    SELECT DISTINCT gbt.appid, gbt.game_name, gbt.tag_name,
           gbt.current_players,
           ROUND(gi.positive * 100.0 / NULLIF(gi.positive + gi.negative, 0), 1) as review_score
    FROM GAMES_BY_TAG gbt
    LEFT JOIN GAME_INFO gi ON gbt.appid = gi.appid
    WHERE gbt.appid != {game_id}
    AND gbt.current_players > 0
    ORDER BY gbt.current_players DESC
    LIMIT 10
    """
    fallback_df = pd.read_sql(fallback_query, conn)
    fallback_df['reason'] = 'Popular game in similar genre.'
    # Rename columns to lowercase for consistency
    fallback_df = fallback_df.rename(columns={
        'APPID': 'appid',
        'GAME_NAME': 'name',
        'TAG_NAME': 'genre',
        'CURRENT_PLAYERS': 'player_count',
        'REVIEW_SCORE': 'review_score'
    })
    return fallback_df, []


@st.cache_data(ttl=60)
def get_live_player_counts(limit=20):
    """Get live player counts with trends"""
    conn = get_snowflake_connection()
    query = f"""
    SELECT
        game_id,
        game_name,
        current_players,
        avg_players_alltime,
        avg_players_24h,
        peak_players,
        trend,
        last_updated
    FROM GAME_PLAYER_COUNTS_LIVE
    ORDER BY current_players DESC
    LIMIT {limit}
    """
    try:
        df = pd.read_sql(query, conn)
        return df
    except:
        return pd.DataFrame()


def render_live_leaderboard():
    """Render the live leaderboard section - powered by GAME_PLAYER_COUNTS table"""
    st.markdown('<p class="section-header">🎮 Live Leaderboard of Games (Streamed)</p>', unsafe_allow_html=True)
    st.caption("Data source: GAME_PLAYER_COUNTS table | Updated every 5 minutes by player_count_producer")

    col1, col2 = st.columns([3, 1])
    with col2:
        limit = st.selectbox("Show top", [25, 50, 100], index=1, key="leaderboard_limit")
        auto_refresh = st.checkbox("Auto-refresh (30s)", value=False, key="auto_refresh")

    df = get_live_leaderboard(limit)

    if not df.empty:
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Games", f"{len(df):,}")
        with col2:
            total_players = df['PLAYER_COUNT'].sum()
            st.metric("Total Players", f"{int(total_players):,}")
        with col3:
            # Count trends
            up_count = len(df[df['TREND'] == 'UP']) if 'TREND' in df.columns else 0
            down_count = len(df[df['TREND'] == 'DOWN']) if 'TREND' in df.columns else 0
            st.metric("Trending", f"{up_count} UP / {down_count} DOWN")
        with col4:
            # Show actual last updated from data
            if 'LAST_UPDATED' in df.columns and not df['LAST_UPDATED'].isna().all():
                last_update = df['LAST_UPDATED'].max()
                st.metric("Last Updated", last_update.strftime("%H:%M:%S") if hasattr(last_update, 'strftime') else str(last_update)[:19])
            else:
                st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))

        # Leaderboard table with trend indicators
        display_df = df.copy()
        if 'TREND' in display_df.columns:
            display_df['TREND'] = display_df['TREND'].map({
                'UP': '📈 UP',
                'DOWN': '📉 DOWN',
                'STABLE': '➡️ STABLE',
                'N/A': '⏳ N/A'
            }).fillna('⏳ N/A')

        st.dataframe(
            display_df.rename(columns={
                'GAME_ID': 'ID',
                'GAME_NAME': 'Game',
                'PLAYER_COUNT': 'Current Players',
                'AVG_PLAYERS_24H': 'Avg 24h',
                'AVG_PLAYERS_ALLTIME': 'Avg All-Time',
                'PEAK_PLAYERS': 'Peak',
                'TREND': 'Trend',
                'DATA_POINTS': 'Samples'
            })[['ID', 'Game', 'Current Players', 'Avg 24h', 'Peak', 'Trend']],
            use_container_width=True,
            height=400
        )
    else:
        st.info("No data available. Please ensure player_count_producer is running.")

    if auto_refresh:
        time.sleep(30)
        st.rerun()


def render_games_by_tag():
    """Render the popular games by tag section"""
    st.markdown('<p class="section-header">🏷️ Popular Games All-Time by Tag (Batch)</p>', unsafe_allow_html=True)

    try:
        tags = get_available_tags()

        col1, col2 = st.columns([2, 1])
        with col1:
            selected_tag = st.selectbox("Select a Tag", tags, key="tag_select")
        with col2:
            limit = st.selectbox("Show top", [10, 20, 50], index=1, key="tag_limit")

        if selected_tag:
            df = get_games_by_tag(selected_tag, limit)

            if not df.empty:
                st.dataframe(
                    df.rename(columns={
                        'TAG_NAME': 'Tag',
                        'GAME_NAME': 'Game',
                        'GAME_ID': 'ID',
                        'TOTAL_DOWNLOADS': 'Downloads',
                        'PRICE_CENTS': 'Price (cents)',
                        'CURRENT_PLAYERS': 'Players'
                    }),
                    use_container_width=True,
                    height=350
                )
            else:
                st.info(f"No games found for tag: {selected_tag}")
    except Exception as e:
        st.warning(f"Games by Tag view not available. Run the SQL views first. Error: {e}")


def render_user_profile():
    """Render the user profile summary section"""
    st.markdown('<p class="section-header">👤 Summary of User Profile ()</p>', unsafe_allow_html=True)

    try:
        # Get available users
        users_df = get_user_profile_summary()

        if not users_df.empty and 'STEAMID' in users_df.columns:
            user_options = dict(zip(users_df['PERSONANAME'].fillna('Unknown'), users_df['STEAMID']))
            selected_user_name = st.selectbox("Select User", list(user_options.keys()), key="user_select")
            selected_user = user_options[selected_user_name]

            if selected_user:
                profile_df = get_user_profile_summary(selected_user)

                if not profile_df.empty:
                    # User stats
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        total_games = len(profile_df)
                        st.metric("Games Owned", total_games)
                    with col2:
                        total_hours = profile_df['HRS_PLAYED'].sum() / 60 if 'HRS_PLAYED' in profile_df.columns else 0
                        st.metric("Total Hours", f"{total_hours:.1f}")
                    with col3:
                        recent_hours = profile_df['RECENT_HRS'].sum() / 60 if 'RECENT_HRS' in profile_df.columns else 0
                        st.metric("Recent 2 Weeks", f"{recent_hours:.1f}h")

                    # Games table
                    st.dataframe(
                        profile_df[['GAME_NAME', 'GAME_TAG', 'HRS_PLAYED', 'RECENT_HRS']].rename(columns={
                            'GAME_NAME': 'Game',
                            'GAME_TAG': 'Genre',
                            'HRS_PLAYED': 'Total Mins',
                            'RECENT_HRS': 'Recent Mins'
                        }),
                        use_container_width=True,
                        height=250
                    )
                else:
                    st.info("No game data for this user")
        else:
            st.info("No user data available. Run the player data ingestion first.")
    except Exception as e:
        st.warning(f"User profile data not available. Error: {e}")


def render_sentiment_generator():
    """Render the game sentiment generator section"""
    st.markdown('<p class="section-header">💬 Recent Game Sentiment Generator (Airflow Batch)</p>', unsafe_allow_html=True)
    st.caption("Results served by VADER Sentiment Analysis | Inputs from Review_DB")

    try:
        df = get_game_sentiments(20)

        if not df.empty:
            # Top sentiment metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                avg_sentiment = df['SENTIMENT_SCORE'].mean()
                st.metric("Avg Sentiment Score", f"{avg_sentiment:.1f}/100")
            with col2:
                avg_positive = df['POSITIVE_PCT'].mean()
                st.metric("Avg Positive %", f"{avg_positive:.1f}%")
            with col3:
                total_reviews = df['TOTAL_REVIEWS_ANALYZED'].sum()
                st.metric("Total Reviews", f"{total_reviews:,}")

            # Sentiment table with color coding
            st.dataframe(
                df.rename(columns={
                    'GAME_ID': 'ID',
                    'GAME_NAME': 'Game',
                    'TOTAL_REVIEWS_ANALYZED': 'Reviews',
                    'POSITIVE_PCT': 'Positive %',
                    'NEGATIVE_PCT': 'Negative %',
                    'SENTIMENT_SCORE': 'Score'
                }),
                use_container_width=True,
                height=300
            )

            # Sample reviews for selected game
            st.markdown("#### Sample Reviews")
            game_options = dict(zip(df['GAME_NAME'], df['GAME_ID']))
            selected_game = st.selectbox("Select game to see reviews", list(game_options.keys()), key="sentiment_game")

            if selected_game:
                reviews_df = get_game_reviews_sample(game_options[selected_game], 5)
                if not reviews_df.empty:
                    for _, row in reviews_df.iterrows():
                        sentiment = "👍" if row['VOTED_UP'] else "👎"
                        st.markdown(f"""
                        {sentiment} **{row['GAME_NAME']}** - {row['PLAYTIME_AT_REVIEW']//60}h played
                        > {row['REVIEW_TEXT'][:300]}{'...' if len(str(row['REVIEW_TEXT'])) > 300 else ''}
                        """)
        else:
            st.info("Sentiment analysis not yet available. Run the Airflow sentiment DAG first.")
    except Exception as e:
        st.warning(f"Sentiment data not available. Error: {e}")


def render_game_recommender():
    """Render the game recommendation section"""
    st.markdown('<p class="section-header">🎯 Recommend Me a Game (AI-Powered)</p>', unsafe_allow_html=True)
    st.caption("AI recommendations powered by OpenAI GPT | Analyzes gameplay, genre, and themes")

    try:
        # Get top games for selection
        top_games = get_live_leaderboard(100)

        if not top_games.empty:
            game_options = dict(zip(top_games['GAME_NAME'], top_games['GAME_ID']))

            selected_game = st.selectbox(
                "Select a game you like",
                list(game_options.keys()),
                key="recommend_game"
            )

            if selected_game:
                game_id = game_options[selected_game]

                with st.spinner("Analyzing games with AI..."):
                    recommendations_df, raw_recommendations = get_game_recommendations(game_id)

                if not recommendations_df.empty:
                    st.markdown(f"**Because you like {selected_game}, you might enjoy:**")

                    # Display each recommendation as a card with reason
                    for _, row in recommendations_df.iterrows():
                        with st.container():
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.markdown(f"### {row['name']}")
                                reason = row.get('reason', 'Similar gameplay and genre.')
                                st.markdown(f"*Why:* {reason}")
                                genre = row.get('genre', 'Unknown')
                                if genre and genre != '[]':
                                    st.caption(f"Genre: {genre}")
                            with col2:
                                players = row.get('player_count', 0)
                                score = row.get('review_score', 0)
                                st.metric("Players", f"{int(players):,}" if players else "N/A")
                                st.metric("Score", f"{score}%" if score else "N/A")
                            st.markdown("---")
                else:
                    st.info("No similar games found. Try a different game.")
        else:
            st.info("No game data available for recommendations.")
    except Exception as e:
        st.warning(f"Recommendation engine not available. Error: {e}")


def main():
    """Main dashboard layout"""
    st.markdown('<p class="main-header">🎮 Steam Analytics Dashboard</p>', unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/8/83/Steam_icon_logo.svg", width=50)
        st.markdown("### Navigation")

        page = st.radio(
            "Select View",
            ["Full Dashboard", "Live Leaderboard", "Games by Tag", "User Profile", "Sentiment Analysis", "Game Recommender"],
            key="nav"
        )

        st.markdown("---")
        st.markdown("### Data Status")

        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM GAME_INFO")
            game_count = cursor.fetchone()[0]
            st.success(f"✅ Connected | {game_count:,} games")
        except Exception as e:
            st.error(f"❌ Connection Error")

        st.markdown("---")
        st.markdown("### About")
        st.markdown("""
        This dashboard displays real-time Steam gaming analytics:
        - **Streamed**: Live player counts
        - **Batch**: Sentiment analysis, recommendations
        - **Micro-batch**: User profiles
        """)

    # Main content based on selection
    if page == "Full Dashboard":
        # Two-column layout matching the design
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("### Generalized Section")
            render_live_leaderboard()
            st.markdown("---")
            render_games_by_tag()

        with col_right:
            st.markdown("### Personalized Section")
            render_user_profile()
            st.markdown("---")
            render_sentiment_generator()
            st.markdown("---")
            render_game_recommender()

    elif page == "Live Leaderboard":
        render_live_leaderboard()

    elif page == "Games by Tag":
        render_games_by_tag()

    elif page == "User Profile":
        render_user_profile()

    elif page == "Sentiment Analysis":
        render_sentiment_generator()

    elif page == "Game Recommender":
        render_game_recommender()


if __name__ == "__main__":
    main()
