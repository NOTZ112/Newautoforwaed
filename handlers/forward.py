import asyncio
import logging

from telegram import Update
from telegram.error import Forbidden, BadRequest, RetryAfter, TimedOut
from telegram.ext import ContextTypes

import database as db

logger = logging.getLogger(__name__)

MAX_RETRIES = 6
MAX_CONCURRENT_SENDS = 5


def _passes_filter(message, filter_type: str) -> bool:
    is_media = bool(
        message.photo
        or message.video
        or message.document
        or message.audio
        or message.animation
        or message.voice
        or message.video_note
        or message.sticker
    )

    if filter_type == "all":
        return True

    if filter_type == "media":
        return is_media

    if filter_type == "text":
        return not is_media and bool(message.text)

    return True


def _hits_blacklist(message, blacklist) -> bool:
    if not blacklist:
        return False

    content = (message.text or message.caption or "").lower()
    return any(word in content for word in blacklist)


async def _send_with_retry(
    bot,
    source_id,
    target_id,
    message,
    mode,
):
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            if mode == "clean":
                await bot.copy_message(
                    chat_id=target_id,
                    from_chat_id=source_id,
                    message_id=message.message_id,
                )
            else:
                await bot.forward_message(
                    chat_id=target_id,
                    from_chat_id=source_id,
                    message_id=message.message_id,
                )

            return True, None

        except RetryAfter as e:
            logger.warning(
                "Flood control: waiting %.1fs to send to %s",
                e.retry_after,
                target_id,
            )

            await asyncio.sleep(e.retry_after + 1)

        except TimedOut:
            attempt += 1

            await asyncio.sleep(
                min(2 * attempt, 20)
            )

        except Forbidden as e:
            # Target is unavailable / bot lost access.
            return False, ("target_unavailable", str(e))

        except BadRequest as e:
            error_text = str(e)

            # -------------------------------------------------
            # MessageIdInvalid
            #
            # The source message is unavailable/invalid.
            # DO NOT remove the target.
            # DO NOT notify the owner.
            # Simply skip this message.
            # -------------------------------------------------
            if "MessageIdInvalid" in error_text:
                return False, ("message_invalid", "")

            # -------------------------------------------------
            # Target no longer available / access lost
            # -------------------------------------------------
            target_errors = (
                "Chat not found",
                "chat not found",
                "Channel not found",
                "channel not found",
                "Group not found",
                "group not found",
                "bot was kicked",
                "bot is not a member",
                "not enough rights",
                "have no rights",
                "Forbidden",
            )

            if any(err in error_text for err in target_errors):
                return False, ("target_unavailable", error_text)

            # Other BadRequest
            return False, ("badrequest", error_text)

        except Exception:
            logger.exception(
                "Unexpected forward error sending to %s",
                target_id,
            )

            attempt += 1

            await asyncio.sleep(
                min(2 * attempt, 20)
            )

    return False, ("retries_exhausted", "")


async def _handle_one_target(
    bot,
    source_id,
    target_id,
    message,
    mode,
):
    ok, err = await _send_with_retry(
        bot,
        source_id,
        target_id,
        message,
        mode,
    )

    return target_id, ok, err


async def channel_post_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = (
        update.channel_post
        or update.edited_channel_post
    )

    if not message:
        return

    source_id = message.chat_id

    source = await db.get_source(source_id)

    if not source or not source.get("active", True):
        return

    if not _passes_filter(
        message,
        source.get("filter_type", "all"),
    ):
        return

    if _hits_blacklist(
        message,
        source.get("blacklist", []),
    ):
        return

    targets = source.get("targets", [])

    if not targets:
        return

    mode = source.get("mode", "clean")
    bot = context.bot

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_SENDS
    )

    async def _bounded(target_id):
        async with semaphore:
            return await _handle_one_target(
                bot,
                source_id,
                target_id,
                message,
                mode,
            )

    results = await asyncio.gather(
        *(
            _bounded(target_id)
            for target_id in list(targets)
        )
    )

    forwarded = 0
    failed = 0

    for target_id, ok, err in results:

        if ok:
            forwarded += 1
            continue

        failed += 1

        if not err:
            continue

        reason, detail = err

        # =====================================================
        # INVALID SOURCE MESSAGE
        #
        # Do absolutely nothing.
        # Do not remove target.
        # Do not notify owner.
        # =====================================================
        if reason == "message_invalid":
            continue

        # =====================================================
        # TARGET UNAVAILABLE
        #
        # Automatically remove unavailable target from DB.
        # Owner notification is intentionally disabled.
        # =====================================================
        if reason == "target_unavailable":

            try:
                await db.deactivate_target(
                    source_id,
                    target_id,
                )

                logger.warning(
                    "Target removed | Source=%s | Target=%s | Reason=%s",
                    source_id,
                    target_id,
                    detail,
                )

            except Exception:
                logger.exception(
                    "Failed to deactivate target %s",
                    target_id,
                )

            continue

        # =====================================================
        # OTHER FORWARD ERRORS
        #
        # Keep these only in Render logs.
        # Nothing is sent to the owner.
        # =====================================================
        logger.warning(
            "Forward failed | Message=%s | Source=%s | Target=%s | Reason=%s | Details=%s",
            message.message_id,
            source_id,
            target_id,
            reason,
            detail,
        )

    await db.bump_stats(
        forwarded=forwarded,
        failed=failed,
    )
