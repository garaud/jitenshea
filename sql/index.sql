

-- bordeaux.timeserie
CREATE INDEX idx_bordeaux_timeseries_id ON bordeaux.timeserie(ident);
CREATE INDEX idx_bordeaux_timeseries_ts ON bordeaux.timeserie(ts);
-- bordeaux.daily_transaction
CREATE INDEX idx_bordeaux_transaction_id ON bordeaux.daily_transaction(id);
CREATE INDEX idx_bordeaux_transaction_date ON bordeaux.daily_transaction(date);


-- lyon.timeserie
CREATE INDEX idx_lyon_timeseries_id ON lyon.timeserie(number);
CREATE INDEX idx_lyon_timeseries_ts ON lyon.timeserie(last_update);

-- lyon.daily_transaction
CREATE INDEX idx_lyon_transaction_id ON lyon.daily_transaction(id);
CREATE INDEX idx_lyon_transaction_date ON lyon.daily_transaction(date);



