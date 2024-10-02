import asyncio
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import aiohttp
from pydantic import BaseModel, field_validator, computed_field, TypeAdapter

from bot.utils.logger import logger
from bot.utils.scripts import escape_html

MISSIONS_FILENAME = 'missions.csv'


class ActiveMissionItem(BaseModel):
    type: str
    verified: bool = False
    verified_at: datetime = None

    @field_validator("verified_at", mode="before")
    @classmethod
    def to_dt(cls, raw: Any) -> datetime:
        if isinstance(raw, int):
            return datetime.fromtimestamp(raw/1000)

    def is_started(self) -> bool:
        return self.verified_at is not None

    def remain_time(self) -> int:
        return int((datetime.now() - self.verified_at).total_seconds())


class MissionItem(BaseModel):
    name: str
    require_answer: bool
    title: str | None = None
    wait_duration_s: int | None = None


class MissionId(BaseModel):
    id: str


class ActiveMission(MissionId):
    items: list[ActiveMissionItem]


class Mission(MissionId):
    title: str | None = None
    reward: int | None = None
    items: list[MissionItem]
    start_at: datetime = None

    @computed_field
    def num(self) -> int:
        return int(self.id.replace('M', ''))

    @field_validator("start_at", mode="before")
    @classmethod
    def to_dt(cls, raw: Any) -> datetime:
        if isinstance(raw, int):
            return datetime.fromtimestamp(raw/1000)
        if isinstance(raw, dict):
            return datetime.fromtimestamp(int(raw.get('$numberDecimal', 0))/1000)


def load_answers_from_file() -> dict:
    ret = defaultdict(list)
    with open(MISSIONS_FILENAME, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        for (task_id, task_name, code) in reader:
            ret[task_id].append(code)

    return ret


def save_missions_to_file(missions):
    answers = load_answers_from_file()

    with open(MISSIONS_FILENAME, 'w', encoding='utf-8-sig', newline='') as f:
        mission_writer = csv.writer(f, delimiter=';')

        for mission in sorted(missions, key=lambda m: m.start_at if m.start_at else datetime.now()):
            for i, _ in enumerate(mission.items):
                mission_writer.writerow([mission.id, mission.title, answers.get(mission.id, [''])[i]])


async def join_mission(self, http_client: aiohttp.ClientSession,
                       mission_id: str) -> dict:
    response_text = ''
    try:
        response = await http_client.post(url='https://api.tapswap.club/api/missions/join_mission',
                                          json={'id': mission_id})
        response_text = await response.text()
        response.raise_for_status()

        return await response.json()
    except Exception as error:
        logger.error(f"{self.session_name} | Unknown error when Join {mission_id} mission: {escape_html(str(error))} | "
                     f"Response text: {escape_html(response_text)[:128]}...")
        await asyncio.sleep(delay=3)

        return {}

async def finish_mission_item(self, http_client: aiohttp.ClientSession,
                              mission_id: str, item_index: int, user_input: str = '') -> dict:
    response_text = ''
    try:
        json_data = {'id': mission_id, 'itemIndex': item_index}
        if user_input:
            json_data.update({'user_input': user_input})
        response = await http_client.post(url='https://api.tapswap.club/api/missions/finish_mission_item',
                                          json=json_data)
        response_text = await response.text()
        response_json = await response.json()
        if response.status == 400 and response_json.get('message', '') == 'invalid_answer':
            logger.error(f"{self.session_name} | Wrong answer for Mission Item <m>{mission_id}/{item_index}</m>: <r>{user_input}</r>")
            return {}

        response.raise_for_status()

        return response_json
    except Exception as error:
        logger.error(f"{self.session_name} | Unknown error when Finish Mission Item [{mission_id}/{item_index}]: {escape_html(str(error))} | "
                     f"Response text: {escape_html(response_text)[:128]}...")
        await asyncio.sleep(delay=3)
        return {}


async def finish_mission(self, http_client: aiohttp.ClientSession,
                         mission_id: str) -> dict:
    response_text = ''
    try:
        response = await http_client.post(url='https://api.tapswap.club/api/missions/finish_mission',
                                          json={'id': mission_id})
        response_text = await response.text()
        response.raise_for_status()

        return await response.json()
    except Exception as error:
        logger.error(f"{self.session_name} | Unknown error when Finish Mission {mission_id}: {escape_html(str(error))} | "
                     f"Response text: {escape_html(response_text)[:128]}...")
        await asyncio.sleep(delay=3)
        return {}


def get_cinema_missions(account_data: dict) -> list[Mission]:
    ta = TypeAdapter(list[Mission])
    missions = ta.validate_python(account_data['conf']['missions'])
    return list(filter(lambda m: m.num >= 1000, missions))


def get_visible_cinema_missions(account_data: dict) -> list[Mission]:
    cinema_missions = get_cinema_missions(account_data)

    # save_missions_to_file(missions=cinema_missions)

    completed = account_data['account']['missions'].get('completed', [])

    actual = list(filter(lambda m: m.id not in completed, cinema_missions))
    return sorted(actual, key=lambda m: m.start_at if m.start_at else datetime.now(), reverse=True)[:4]


def get_active_missions(account_data: dict) -> dict[str, ActiveMission]:
    account_missions = account_data['account']['missions']
    ta2 = TypeAdapter(list[ActiveMission])
    return {m.id: m for m in ta2.validate_python(account_missions.get('active', []))}


async def complete_cinema_mission(self, http_client: aiohttp.ClientSession,
                                  mission: Mission, account_data: dict) -> dict:
    logger.info(f"{self.session_name} | Processing Cinema Mission <m>{mission.id}</m>"
                f" <g>{mission.title}</g>")
    answers = load_answers_from_file()
    active_missions = get_active_missions(account_data)

    if mission.id not in active_missions:
        logger.info(f"{self.session_name} | Sleep 5s before Start Mission <m>{mission.id}</m>")
        await asyncio.sleep(delay=5)

        if account_data := await join_mission(self, http_client=http_client, mission_id=mission.id):
            active_missions = get_active_missions(account_data)
            if mission.id in active_missions:
                logger.info(f"{self.session_name} | Successfully started Mission <m>{mission.id}</m>")
            else:
                logger.warning(f"{self.session_name} | Error starting Mission <m>{mission.id}</m>, skipping...")
                return {}
        else:
            return {}

    active_mission = active_missions[mission.id]

    for i, item in enumerate(mission.items):
        active_item = active_mission.items[i]
        if not active_item.verified and not active_item.is_started():
            logger.info(f"{self.session_name} | Sleep 5s before Start Mission Item <m>{mission.id}/{i}</m>")
            await asyncio.sleep(delay=5)
            if account_data := await finish_mission_item(self, http_client=http_client,
                                                         mission_id=mission.id, item_index=i):
                wait_seconds = item.wait_duration_s
                logger.info(f"{self.session_name} | Successfully started Mission Item <m>{mission.id}/{i}</m>")
                active_mission = get_active_missions(account_data)[mission.id]
            else:
                continue
        else:
            wait_seconds = item.wait_duration_s - active_item.remain_time()

        answer = None
        if item.require_answer:
            if mission.id in answers:
                mission_answers = answers[mission.id]
                answer = mission_answers[i]

            if not answer:
                logger.warning(f"{self.session_name} | Missing answer for Mission Item <m>{mission.id}/{i}</m>")
                continue

        if wait_seconds > 0:
            logger.info(
                f"{self.session_name} | Wait for validating Mission Item <m>{mission.id}/{i}</m>: {wait_seconds}s...")
            await asyncio.sleep(delay=wait_seconds)

        if not active_item.verified and \
                (account_data := await finish_mission_item(self, http_client=http_client,
                                                           mission_id=mission.id, item_index=i, user_input=answer)):
            logger.info(f"{self.session_name} | Successfully finished Mission Item <m>{mission.id}/{i}</m>")
            active_mission = get_active_missions(account_data)[mission.id]

    if all(item.verified for item in active_mission.items):
        logger.info(f"{self.session_name} | Sleep 5s before Finish Mission <m>{mission.id}</m>")
        await asyncio.sleep(delay=5)
        if account_data := await finish_mission(self, http_client=http_client, mission_id=mission.id):
            player_data = account_data['player']
            logger.info(
                f"{self.session_name} | Successfully finished Mission <m>{mission.id}</m>: videos <m>{player_data['videos']}</m>")

    return account_data
