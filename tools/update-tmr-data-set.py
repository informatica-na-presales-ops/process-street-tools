import logging
import os
import signal
import sys
import time
import types
import typing

import apscheduler.schedulers.blocking
import datime
import httpx
import notch
import psycopg2.extras

log = logging.getLogger(__name__)

PRST_DATA_SET_WEBHOOK = os.getenv("PRST_DATA_SET_WEBHOOK")


def get_pg_connection() -> psycopg2._psycopg.connection:
    cnx = psycopg2.connect(
        os.getenv("DB"), cursor_factory=psycopg2.extras.RealDictCursor
    )
    return cnx


def yield_pg_records(cnx: psycopg2._psycopg.connection) -> typing.Iterable[dict]:
    sql = """--sql
        select
            emp_id,
            emp_job_code,
            emp_name,
            emp_profile,
            mgr_email,
            mgr_name,
            emp_email,
            emp_hierarchy_designation,
            mgr_hierarchy_designation,
            peer_group_name,
            l2_group_name,
            l3_group_name,
            peer_group_email,
            l2_group_email,
            l3_group_email
        from v_prst_tmr_data_set
        order by emp_id
    """
    with cnx:
        cur: psycopg2.extras.RealDictCursor
        with cnx.cursor() as cur:
            cur.execute(sql)
            yield from cur.fetchall()


def send_to_webbook(record: dict) -> None:
    with httpx.Client() as client:
        try_again = True
        while try_again:
            try_again = False
            r = client.post(PRST_DATA_SET_WEBHOOK, json=record)
            if r.status_code == 429:
                try_again = True
                wait_seconds = int(r.headers["Retry-After"]) + 1
                log.warning(f"Too many requests, waiting {wait_seconds} seconds...")
                time.sleep(wait_seconds)
            else:
                r.raise_for_status()


def main_job(repeat_interval_hours: int | None = None) -> None:
    start = time.monotonic()
    log.info("update-tmr-data-set starting")

    cnx = get_pg_connection()
    count = 0
    for record in yield_pg_records(cnx):
        count += 1
        log.info(f"Processing record {count}: {record['emp_id']}")
        send_to_webbook(record)

    if repeat_interval_hours:
        plural = "" if repeat_interval_hours == 1 else "s"
        repeat_message = f"see you again in {repeat_interval_hours} hour{plural}"
    else:
        repeat_message = "quitting"
    duration = datime.pretty_duration_short(int(time.monotonic() - start))
    log.info(f"update-tmr-data-set completed in {duration}, {repeat_message}")


def main() -> None:
    notch.configure()
    repeat = os.getenv("REPEAT", "false").lower() in ("1", "on", "true", "yes")
    if repeat:
        repeat_interval_hours = int(os.getenv("REPEAT_INTERVAL_HOURS", "1"))
        plural = "" if repeat_interval_hours == 1 else "s"
        log.info(f"This job will repeat every {repeat_interval_hours} hour{plural}")
        log.info(
            "Change this value by setting the "
            "REPEAT_INTERVAL_HOURS environment variable"
        )
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(
            main_job,
            "interval",
            args=[repeat_interval_hours],
            hours=repeat_interval_hours,
        )
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal: int, _frame: types.FrameType) -> None:
    sys.exit()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
