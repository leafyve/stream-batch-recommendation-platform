from __future__ import annotations

from datetime import UTC, datetime, timedelta

from recommendation_features.feature_definitions import aggregate_features, event_windows
from recommendation_features.schemas import RatingEvent


def make_event(
    user_id: int, movie_id: int, rating: float, event_time: str, replay: str = "run"
) -> RatingEvent:
    return RatingEvent.create(
        user_id=user_id,
        movie_id=movie_id,
        rating=rating,
        event_time=event_time,
        ingestion_time="2026-01-10T00:00:00Z",
        source="synthetic",
        replay_run_id=replay,
    )


def test_event_belongs_to_seven_daily_windows() -> None:
    windows = event_windows("2026-01-08T12:30:00Z")
    assert len(windows) == 7
    assert windows[0] == (
        datetime(2026, 1, 8, tzinfo=UTC),
        datetime(2026, 1, 15, tzinfo=UTC),
    )
    assert windows[-1][0] == datetime(2026, 1, 2, tzinfo=UTC)
    assert all(end - start == timedelta(days=7) for start, end in windows)


def test_duplicate_event_is_counted_once() -> None:
    original = make_event(1, 10, 4.0, "2026-01-08T12:00:00Z", "first")
    duplicate = make_event(1, 10, 4.0, "2026-01-08T12:00:00Z", "replay")
    rows = aggregate_features([original, duplicate], "user")
    assert len(rows) == 7
    assert all(row.rating_count == 1 for row in rows)


def test_out_of_order_input_produces_deterministic_features() -> None:
    early = make_event(1, 10, 2.0, "2026-01-08T01:00:00Z")
    late = make_event(1, 11, 4.0, "2026-01-09T01:00:00Z")
    ordered = aggregate_features([early, late], "user")
    reversed_rows = aggregate_features([late, early], "user")
    assert ordered == reversed_rows
    overlapping = [row for row in ordered if row.rating_count == 2]
    assert overlapping
    assert all(row.average_rating == 3.0 for row in overlapping)
    assert all(row.minimum_rating == 2.0 and row.maximum_rating == 4.0 for row in overlapping)


def test_movie_and_user_keys_are_independent() -> None:
    events = [
        make_event(1, 99, 3.0, "2026-01-08T00:00:00Z"),
        make_event(2, 99, 5.0, "2026-01-08T01:00:00Z"),
    ]
    users = aggregate_features(events, "user")
    movies = aggregate_features(events, "movie")
    assert {row.entity_id for row in users} == {1, 2}
    assert {row.entity_id for row in movies} == {99}
    assert all(row.rating_count == 2 for row in movies)

