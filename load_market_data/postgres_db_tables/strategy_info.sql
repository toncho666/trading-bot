DROP TABLE IF EXISTS test.strategy_info;

CREATE TABLE IF NOT EXISTS test.strategy_info (
    id INT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    asset TEXT NOT NULL,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expirationDate TIMESTAMP
);

-- Наполняем таблицу тестовыми данными (4 записи)
INSERT INTO test.strategy_info (id, name, description, timeframe, asset, createdAt, expirationDate) VALUES
(1, 'close_open_1pct', 'Generate binary signals based on the percentage change between close and open prices exceeding a fixed threshold.', 'h1', 'BTCUSDT', '04-05-2026'::date, NOW() + INTERVAL '90 days'),
(2, 'fractal', 'Generate trend-following signals using dual EMA crossover confirmed by shifted fractal extremes.', 'h1', 'BTCUSDT', '04-05-2026'::date , NOW() + INTERVAL '90 days'),
(3, 'macd_hist', 'Generate reversal signals when moving average divergence peaks and begins contracting from an extreme.', 'h1', 'BTCUSDT', '04-05-2026'::date , NOW() + INTERVAL '90 days'),
(4, 'candles', 'Generate buy/sell signals based on candlestick patterns with optional volume confirmation and filtering.', 'h1', 'BTCUSDT', '04-05-2026'::date , NOW() + INTERVAL '90 days'),
(5, 'close_open_engulfing', 'Generate contrarian signals based on prior strong momentum reversal confirmed by current candle direction.', 'h1', 'BTCUSDT', '04-05-2026'::date , NOW() + INTERVAL '90 days');

-- Проверяем результат
SELECT * FROM test.strategy_info;
    
