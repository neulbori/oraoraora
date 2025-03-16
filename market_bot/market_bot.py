import discord
from discord import app_commands
from discord.ext import commands
import pandas as pd
import requests
import aiohttp
import asyncio
import os
import time
import traceback
import re
from rapidfuzz import process, fuzz
import json
import logging

DISCORD_TOKEN = os.environ.get('DISCORD_TOKEN')

# 서버 ID와 이름 매핑
SERVER_ID_NAME_MAP = {
    2075: "카벙클",
    2076: "초코보",
    2077: "모그리",
    2078: "톤베리",
    2080: "펜리르"
}

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("market_bot.log", encoding="utf-8"), 
        logging.StreamHandler()
    ]
)

ITEM_LIST_FILE = "data.dat"
CACHE_FILE = "item_cache.json"

if not DISCORD_TOKEN:
    raise ValueError("디스코드 봇 토큰이 환경 변수에 설정되지 않았습니다.")

UNIVERSALIS_API_BASE = "https://universalis.app/api/v2"

intents = discord.Intents.default()
intents.message_content = True 

CACHE = {}  # { (server_id, item_id): { "data": market_data, "timestamp": time.time() } }
CACHE_EXPIRY = 1 * 60  # 1분


def save_cache_to_file(cache): # 아이템 이름 검색 결과 캐싱
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=4)

def load_cache_from_file():
    if not os.path.exists(CACHE_FILE):
        logging.warning(f"캐시 파일 '{CACHE_FILE}'이(가) 존재하지 않습니다. 새로 생성합니다.")
        return {}
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            if os.stat(CACHE_FILE).st_size == 0:
                logging.warning(f"캐시 파일 '{CACHE_FILE}'이(가) 비어 있습니다.")
                return {}
            return json.load(f)
    except json.JSONDecodeError as e:
        # 손상된 JSON 파일 위치 출력
        logging.error(f"캐시 파일 '{CACHE_FILE}'이(가) 손상되었습니다: {e}")
        logging.error(f"손상된 위치: 줄 번호 {e.lineno}, 열 번호 {e.colno}")
        
        # 손상된 줄 읽기
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if e.lineno <= len(lines):
                corrupted_line = lines[e.lineno - 1].strip()
                logging.error(f"손상된 줄 내용: {corrupted_line}")


item_cache = load_cache_from_file()

def normalize_text(text):
    if not isinstance(text, str):
        return "" 
    return re.sub(r'\s+', '', text).lower()

def fuzzy_match_and_contains(item_df, column, item_text, threshold=65):
    item_df = item_df.copy()

    item_df['normalized_name'] = item_df[column].apply(normalize_text)

    normalized_text = normalize_text(item_text)
    fuzzy_matches = process.extract(normalized_text, item_df['normalized_name'], scorer=fuzz.ratio, limit=None)
    results = []
    added_indices = set()

    for match in fuzzy_matches:
        if match[1] >= threshold:
            row = item_df[item_df['normalized_name'] == match[0]].iloc[0].to_dict()
            if row not in results:
                results.append(row)

    contains_results = item_df[item_df[column].str.contains(item_text, case=False, na=False, regex=False)].to_dict('records')
    for row in contains_results:
        if row not in results: 
            results.append(row)

    unique_results = pd.DataFrame(results).drop_duplicates(subset=column).reset_index(drop=True)
    return unique_results.drop(columns=['normalized_name'], errors='ignore')


item_df = pd.DataFrame()

try:
    df = pd.read_excel(ITEM_LIST_FILE, engine='openpyxl')
    df.columns = [col.lower() for col in df.columns]
    if 'id' not in df.columns or 'name' not in df.columns or 'isuntradable' not in df.columns or 'icon' not in df.columns:
        logging.info(f"Data 파일 '{ITEM_LIST_FILE}'에 'ID', 'Name', 'isUntradable', 또는 'icon' 열이 없습니다.")

    item_df = df[df['isuntradable'] != True].copy()
    if not item_df.empty:
        if 'normalized_name' not in item_df.columns:
            item_df.loc[:, 'normalized_name'] = item_df['name'].apply(normalize_text)
    else:
        logging.warning("ITEM_LIST_FILE에서 필터링된 데이터가 없습니다.")


except FileNotFoundError:
    logging.info(f"DATA 파일 '{ITEM_LIST_FILE}'을(를) 찾을 수 없습니다. 파일이 올바른 위치에 있는지 확인해주세요.")
except Exception as e:
    logging.info(f"DATA 파일 로드 중 오류 발생: {e}")

def search_items_by_text(item_text: str):
    global item_cache

    # 캐시 확인
    if item_text in item_cache:
        cached_results = item_cache[item_text]
        logging.info(f"'{item_text}'에 대한 캐시 데이터: {cached_results[0]['name']}")
        return cached_results

    if item_df.empty:
        logging.warning("아이템 데이터프레임이 비어 있습니다.")
        return []

    try:
        normalized_text = normalize_text(item_text)
        fuzzy_matches = process.extract(normalized_text, item_df['normalized_name'], scorer=fuzz.ratio, limit=50)

        results = []
        for match in fuzzy_matches:
            matching_row = item_df[item_df['normalized_name'] == match[0]]
            if not matching_row.empty:
                result = matching_row.iloc[0].to_dict()
                result['similarity'] = match[1]

                if not isinstance(result.get('name'), str):
                    return []

                result['is_exact_match'] = (result['name'].lower() == item_text.lower())
                results.append(result)

        contains_results = item_df[item_df['name'].str.contains(item_text, case=False, na=False, regex=False)].to_dict('records')
        for item in contains_results:
            item['similarity'] = 95  # 포함된 항목의 기본 유사도 점수
            item['is_exact_match'] = False
            results.append(item)

        # 완전 일치 항목 우선 처리 (similarity=100)
        for result in results:
            if result['name'].lower() == item_text.lower():
                result['similarity'] = 100
                result['is_exact_match'] = True

        # 중복 제거
        unique_results = {item['id']: item for item in results}.values()

        # 우선순위 정렬
        sorted_results = sorted(unique_results, key=lambda x: (x['is_exact_match'], x['similarity']), reverse=True)

        # 캐시에 저장
        item_cache[item_text] = sorted_results
        save_cache_to_file(item_cache)

        # 검색 결과 반환
        if sorted_results:
            logging.info(f"'{item_text}'에 대한 검색 결과: {sorted_results[0]['name']}")
        else:
            logging.info(f"'{item_text}'에 대한 검색 결과가 없습니다.")

        return sorted_results

    except Exception as e:
        logging.error(f"검색 중 오류 발생: {e}")
        traceback.print_exc()
        return []

def get_cached_data(server_id, item_id): # 시세 캐시 로드
    cache_key = (server_id, item_id)
    if cache_key in CACHE:
        cached_entry = CACHE[cache_key]
        if time.time() - cached_entry["timestamp"] < CACHE_EXPIRY:
            return cached_entry["data"]
        else:
            del CACHE[cache_key]
    return None

def save_to_cache(server_id, item_id, market_data): # 시세 캐싱
    cache_key = (server_id, item_id)
    CACHE[cache_key] = {
        "data": market_data,
        "timestamp": time.time()
    }

def fetch_market_data(session, server_id, server_name, item_id):
    cached_data = get_cached_data(server_id, item_id)
    if cached_data:
        return cached_data

    aggregated_url = f"{UNIVERSALIS_API_BASE}/aggregated/{server_id}/{item_id}"
    response = requests.get(aggregated_url)
    if response.status_code in [404, 400]:
        return None
    response.raise_for_status()
    market_data = response.json()

    save_to_cache(server_id, item_id, market_data)
    return market_data

bot = commands.Bot(command_prefix='/', intents=intents)

@bot.event
async def on_ready():
    logging.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    logging.info('------')
    try:
        synced = await bot.tree.sync()
        logging.info(f"Synced {len(synced)} command(s)")
    except Exception as e:
        logging.error(f"Error syncing commands: {e}")

@bot.tree.command(name="검색", description="FFXIV 아이템의 가격을 검색합니다.")
@app_commands.describe(item_name="검색할 아이템의 이름을 입력하세요.")
async def search(ctx: discord.Interaction, item_name: str):
    try:
        if not ctx.response.is_done():
            await ctx.response.defer()

        logging.info(item_name)
        items = search_items_by_text(item_name)
        
        if not items:
            embed = discord.Embed(title=f"{item_name}", color=0x00ff00)
            embed.description = f"해당 아이템에 대한 데이터가 없습니다."
            await ctx.followup.send(embed=embed)
            return

        item = items[0]

        item_id = item['id']
        item_name = item['name']
        item_icon = item['icon']

        embed = discord.Embed(title=f"{item_name}", color=0x00ff00)
        found = False

        async with aiohttp.ClientSession() as session:
            for server_id, server_name in SERVER_ID_NAME_MAP.items():
                market_data = fetch_market_data(session, server_id, server_name, item_id)
                if market_data:
                    data = market_data['results'][0]
                    hq_data = data.get('hq', {})
                    nq_data = data.get('nq', {})

                    min_price_hq = hq_data.get('minListing', {}).get('world', {}).get('price') if hq_data else None
                    min_price_nq = nq_data.get('minListing', {}).get('world', {}).get('price') if nq_data else None

                    found = found or bool(min_price_hq or min_price_nq)

                    embed_value = (f"**HQ 최저 판매가:** {min_price_hq:,} 길" if min_price_hq else "") + \
                                  ("\n" if min_price_hq and min_price_nq else "") + \
                                  (f"**NQ 최저 판매가:** {min_price_nq:,} 길" if min_price_nq else "")

                    if not embed_value:
                        embed_value = "해당 서버에는 매물이 없습니다."

                    embed.add_field(name=server_name, value=embed_value, inline=False)

            no_icon = False
            if found:
                try:
                    icon_url = f"./icon/{int(item_icon) // 1000 * 1000:06}/{int(item_icon):06}.png"
                    icon_filename = f"{int(item_icon):06}.png"
                    icon = discord.File(icon_url, filename=icon_filename)
                    embed.set_thumbnail(url=f"attachment://{icon_filename}")
                except FileNotFoundError:
                    logging.info(f"{item['id']}의 아이콘 {item['icon']}.png를 찾을 수 없습니다.")
                    no_icon = True
            else:
                embed.description = f"해당 아이템에 대한 데이터가 없습니다."
                no_icon = True
            
            rest = ""

            for i in range(0,len(items)):
                if items[i] == item:
                    continue
                if len(rest)+len(items[i]['name']) >= 100:
                    break
                rest = f"{rest}\n{items[i]['name']}"
            if rest != "":
                embed.set_footer(text=f"다른 아이템을 찾으셨나요?\n{rest}")
                
            if not no_icon:
                await ctx.followup.send(embed=embed, file=icon)
            else:
                await ctx.followup.send(embed=embed)

    except discord.errors.NotFound:
        logging.info("Interaction이 만료되었거나 Webhook이 유효하지 않습니다.")
    except Exception as e:
        # 기타 예상치 못한 오류 처리
        if not ctx.response.is_done():
            await ctx.followup.send("명령 실행 중 오류가 발생했습니다. 다시 시도해 주세요.")
        traceback.print_exc()

bot.run(DISCORD_TOKEN)
