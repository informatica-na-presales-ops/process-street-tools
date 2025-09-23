import os
import time
import typing

import httpx
import psycopg2.extras

PRST_DATA_SET_WEBHOOK = os.getenv("PRST_DATA_SET_WEBHOOK")


def get_pg_connection():
    cnx = psycopg2.connect(
        os.getenv("PG_DSN"), cursor_factory=psycopg2.extras.RealDictCursor
    )
    return cnx


def yield_pg_records(cnx) -> typing.Iterable[dict]:
    sql = """
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
        with cnx.cursor() as cur:
            cur.execute(sql)
            yield from cur.fetchall()


def send_to_webbook(record: dict):
    with httpx.Client(headers={"X-API-Key": os.getenv("PRST_API_KEY")}) as client:
        try_again = True
        while try_again:
            try_again = False
            r = client.post(PRST_DATA_SET_WEBHOOK, json=record)
            if r.status_code == 429:
                try_again = True
                wait_seconds = int(r.headers["Retry-After"]) + 1
                print(f"Too many requests, waiting {wait_seconds} seconds...")
                time.sleep(wait_seconds)
            else:
                r.raise_for_status()


def main():
    cnx = get_pg_connection()
    for record in yield_pg_records(cnx):
        print(f"Processing record: {record['emp_id']}")
        send_to_webbook(record)


if __name__ == "__main__":
    main()
