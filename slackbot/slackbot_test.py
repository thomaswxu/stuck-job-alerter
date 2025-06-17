import pytest
from slackbot import Slackbot

def test_tags_to_text():
    assert (Slackbot.tags_to_text({}) == "None")
    assert (Slackbot.tags_to_text({"key_only": "", "key": "val"}) == "\u2022 *key_only*\n\u2022 *key:* val")
    
def test_blocks_to_payload():
    assert (Slackbot.blocks_to_payload([]) == {"blocks": []})
    assert (Slackbot.blocks_to_payload([{"type": "section", "text": {"type": "mrkdwn", "text": "Hello world"}}])
            == {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "Hello world"}}]})
    