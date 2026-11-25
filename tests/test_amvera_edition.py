import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock

from bs4 import BeautifulSoup
import pytest
from rich.console import Console

from amvera import digest, worker
from src.models import ContentItem
from src.orchestrator import HorizonOrchestrator
from src.storage.manager import StorageManager

DAY = datetime(2026, 11, 25, 7, 30, tzinfo=worker.ZONE)


def test_plan_only_and_sources():
    cfg = digest.configuration("plan-model")
    assert cfg.ai.provider == "openai-chatgpt" and not cfg.ai.provider_chain and not cfg.ai.api_key_env
    assert cfg.ai.languages == ["ru"]
    assert len(cfg.sources.rss) == 12
    assert {c.channel for c in cfg.sources.telegram.channels} == {"telegram", "BotNews"}
    assert len(cfg.digest.category_groups) == 8
    auth = Mock()
    auth.models.return_value = [{"slug": "first"}, {"slug": "second"}]
    assert digest.select_model(auth, "account", "") == "first"
    assert digest.select_model(auth, "account", "second") == "second"
    with pytest.raises(ValueError):
        digest.select_model(auth, "account", "api-only")


def test_full_editorial_pipeline(tmp_path):
    def item(i, url):
        return ContentItem(id=str(i), title="Input", url=url, content="<p>Reported facts</p>",
                           source_type="rss", published_at=DAY, metadata={"category": "Финтех"})
    engine = HorizonOrchestrator(digest.configuration("plan"), StorageManager(str(tmp_path)), console=Console(quiet=True))
    merged = engine.merge_cross_source_duplicates([
        item(0, "https://example.com/n"), item(1, "https://example.com/n?utm_medium=rss"),
        item(2, "https://example.com/z"), item(3, "https://example.com/other")])
    assert len(merged) == 3
    engine.ai_client = Mock()
    engine.ai_client.complete = AsyncMock(side_effect=[
        '{"items":[{"id":0,"score":9,"topic":"Финтех","title":"Оплата <QR>","summary":"Запущен новый способ оплаты."},'
        '{"id":1,"score":8,"topic":"Финтех","title":"Та же новость","summary":"Подтверждён запуск оплаты."},'
        '{"id":2,"score":3}]}', '{"duplicates":[[0,1]]}'])
    stories = asyncio.run(digest.assess(engine, merged))
    assert len(stories) == 1
    message = digest.render(DAY.date(), stories, True)
    assert "Оплата &lt;QR&gt;" in message and "9/10" in message and "временно недоступна" in message
    assert BeautifulSoup(message, "html.parser").a["href"] == "https://example.com/n"


@pytest.mark.parametrize("response", ['{"items":[]}', '{"items":[{"id":1,"score":6}]}',
    '{"items":[{"id":0,"score":8,"topic":"ИИ","title":"News","summary":"All English"}]}'])
def test_rejects_incomplete_or_unlocalized_edition(tmp_path, response):
    engine = HorizonOrchestrator(digest.configuration("plan"), StorageManager(str(tmp_path)))
    engine.ai_client = Mock(complete=AsyncMock(return_value=response))
    story = ContentItem(id="1", title="Title", url="https://example.com", source_type="rss",
                        published_at=DAY, metadata={"category": "ИИ"})
    with pytest.raises(ValueError):
        asyncio.run(digest.assess(engine, [story]))


def test_restart_and_0800_moscow(tmp_path):
    build = AsyncMock(return_value="Текст выпуска")
    call = Mock(return_value={"message_id": 123, "chat": {"id": 42}})
    def run(at, ready=True):
        db = worker.open_db(tmp_path)
        try:
            return worker.tick(at, db, tmp_path, ("bot", "42"), ready, build=build, call=call)
        finally:
            db.close()
    assert run(DAY - timedelta(seconds=1)) == "ready"
    assert run(DAY, False) == "waiting_for_chatgpt"
    assert run(DAY) == "prepared"
    assert run(DAY.replace(minute=59, second=59)) == "prepared"
    assert call.call_count == 0
    assert run(DAY.replace(hour=8, minute=0), False) == "sent"
    assert run(DAY.replace(hour=9)) == "sent"
    assert call.call_count == build.await_count == 1
    assert run(DAY + timedelta(days=1, hours=3)) == "sent"
    assert call.call_count == build.await_count == 2


def test_failure_and_lost_receipt(tmp_path):
    db = worker.open_db(tmp_path)
    at = DAY.replace(hour=8)
    build = AsyncMock(side_effect=[RuntimeError("source outage"), "Новости"])
    call = Mock(side_effect=[worker.TelegramRejected(90), OSError("connection lost")])
    def run(delta):
        return worker.tick(at + timedelta(seconds=delta), db, tmp_path, ("bot", "42"), True, build=build, call=call)
    with pytest.raises(RuntimeError):
        run(0)
    assert run(30) == "telegram_retry"
    assert run(100) == "prepared"
    assert run(125) == "delivery_unconfirmed"
    assert run(180) == "delivery_unconfirmed"
    assert call.call_count == build.await_count == 2
    db.close()
