"""PostgreSQL read model. All queries run in read-only transactions."""
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import Settings


class PostgresReader:
    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def _cursor(self):
        if not self.settings.db_password:
            raise RuntimeError("DB_PASSWORD is not configured")
        conn = psycopg2.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            dbname=self.settings.db_name,
            user=self.settings.db_user,
            password=self.settings.db_password,
            connect_timeout=3,
            application_name="trama-commerce-lab-api",
        )
        try:
            conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
        finally:
            conn.rollback()
            conn.close()

    def events(self, limit: int, offset: int, event_type: str | None, category: str | None):
        with self._cursor() as cursor:
            cursor.execute(
                """SELECT event_id, user_id, product_id, category, event_type,
                          price, quantity, event_timestamp, total_amount
                   FROM events
                   WHERE (%s IS NULL OR event_type = %s)
                     AND (%s IS NULL OR category = %s)
                   ORDER BY event_timestamp DESC, event_id DESC
                   LIMIT %s OFFSET %s""",
                (event_type, event_type, category, category, limit, offset),
            )
            return list(cursor.fetchall())

    def window_stats(self, limit: int, offset: int, category: str | None):
        with self._cursor() as cursor:
            cursor.execute(
                """SELECT window_start, window_end, category, total_events,
                          purchases, revenue
                   FROM window_stats
                   WHERE (%s IS NULL OR category = %s)
                   ORDER BY window_start DESC, category
                   LIMIT %s OFFSET %s""",
                (category, category, limit, offset),
            )
            return list(cursor.fetchall())

    def metrics(self):
        with self._cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*) AS total_events,
                          COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                          COALESCE(SUM(total_amount), 0) AS revenue
                   FROM events"""
            )
            totals = dict(cursor.fetchone())
            cursor.execute(
                """SELECT category, COUNT(*) AS total_events,
                          COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchases,
                          COALESCE(SUM(total_amount), 0) AS revenue
                   FROM events GROUP BY category ORDER BY category"""
            )
            totals["categories"] = list(cursor.fetchall())
            cursor.execute(
                """SELECT event_type, COUNT(*) AS total_events
                   FROM events GROUP BY event_type ORDER BY event_type"""
            )
            totals["event_types"] = list(cursor.fetchall())
            return totals

    def consistency(self):
        with self._cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*) AS total,
                          COALESCE(SUM(total_amount), 0) AS revenue
                   FROM events"""
            )
            events = cursor.fetchone()
            cursor.execute(
                """SELECT COALESCE(SUM(total_events), 0) AS total,
                          COALESCE(SUM(revenue), 0) AS revenue
                   FROM window_stats"""
            )
            windows = cursor.fetchone()
            cursor.execute(
                """SELECT COUNT(*) FROM (
                       SELECT event_id FROM events
                       GROUP BY event_id HAVING COUNT(*) > 1
                   ) AS duplicates"""
            )
            duplicate_ids = cursor.fetchone()["count"]
        count_difference = events["total"] - windows["total"]
        revenue_difference = events["revenue"] - windows["revenue"]
        return {
            "consistent": count_difference == 0 and revenue_difference == 0 and duplicate_ids == 0,
            "events_total": events["total"],
            "windows_total": windows["total"],
            "events_revenue": events["revenue"],
            "windows_revenue": windows["revenue"],
            "duplicate_event_ids": duplicate_ids,
            "event_count_difference": count_difference,
            "revenue_difference": revenue_difference,
        }
