MODEL (
  name matrix.seed_model,
  kind SEED (
    path '../seeds/source.csv',
    batch_size 2
  ),
  columns (
    id INTEGER,
    name TEXT,
    event_date DATE,
    updated_at TIMESTAMP,
    amount INTEGER
  ),
  grain id
);
