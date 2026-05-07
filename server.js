const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const PORT = process.env.PORT || 3000;

// Timeframes PocketOption (en secondes → ms)
const TIMEFRAMES_MS = {
    '5s': 5000,
    '10s': 10000,
    '15s': 15000,
    '30s': 30000,
    '60s': 60000,
    '120s': 120000,
    '300s': 300000
};
const TIMEFRAMES = Object.keys(TIMEFRAMES_MS);

// Mapping : symbole PocketOption → symbole Binance
const ASSET_MAP = {
    'EURUSD_otc': 'EURUSDT',
    'GBPUSD_otc': 'GBPUSDT',
    'USDJPY_otc': 'USDJPY',
    'AUDUSD_otc': 'AUDUSDT',
    'USDCAD_otc': 'USDCAD',
    'BTCUSD_otc': 'BTCUSDT',
    'ETHUSD_otc': 'ETHUSDT',
    'EURGBP_otc': 'EURGBP',
    'GBPJPY_otc': 'GBPJPY',
    'EURJPY_otc': 'EURJPY'
};
const ASSETS = Object.keys(ASSET_MAP);

// Stockage
const candles = {};
const lastPrices = {};
const connected = { value: false };

ASSETS.forEach(s => {
    candles[s] = {};
    TIMEFRAMES.forEach(tf => candles[s][tf] = []);
    lastPrices[s] = 0;
});

// Indicateurs
function calculateRSI(prices, period = 14) {
    if (prices.length < period + 1) return 50;
    let gains = 0, losses = 0;
    for (let i = prices.length - period; i < prices.length; i++) {
        const diff = prices[i] - prices[i - 1];
        if (diff >= 0) gains += diff;
        else losses -= diff;
    }
    const avgGain = gains / period, avgLoss = losses / period;
    if (avgLoss === 0) return 100;
    return 100 - (100 / (1 + avgGain / avgLoss));
}

function calculateEMA(data, period) {
    if (data.length < period) return data[data.length - 1] || 0;
    const k = 2 / (period + 1);
    let ema = data.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < data.length; i++) ema = data[i] * k + ema * (1 - k);
    return ema;
}

function calculateMACD(closes) {
    if (closes.length < 26) return { macd: 0, signal: 0, histogram: 0 };
    const ema12 = calculateEMA(closes, 12);
    const ema26 = calculateEMA(closes, 26);
    const macdLine = ema12 - ema26;
    const macdHistory = [];
    for (let i = 25; i < closes.length; i++) {
        macdHistory.push(calculateEMA(closes.slice(0, i + 1), 12) - calculateEMA(closes.slice(0, i + 1), 26));
    }
    const signalLine = calculateEMA(macdHistory, 9);
    return { macd: macdLine, signal: signalLine, histogram: macdLine - signalLine };
}

function getTradeSignal(symbol, timeframe) {
    const closes = candles[symbol][timeframe].map(c => c.close);
    if (closes.length < 50) return { signal: 'NEUTRE', confidence: 0 };
    const currentPrice = closes[closes.length - 1];
    const rsi = calculateRSI(closes, 14);
    const ema20 = calculateEMA(closes, 20);
    const ema50 = calculateEMA(closes, 50);
    const { macd, signal: macdSignal, histogram } = calculateMACD(closes);
    let buyScore = 0, sellScore = 0;
    if (rsi < 30) buyScore += 25; else if (rsi > 70) sellScore += 25;
    if (currentPrice > ema20 && ema20 > ema50) buyScore += 20;
    else if (currentPrice < ema20 && ema20 < ema50) sellScore += 20;
    if (histogram > 0 && macd > macdSignal) buyScore += 20;
    else if (histogram < 0 && macd < macdSignal) sellScore += 20;
    const recentLow = Math.min(...closes.slice(-30));
    const recentHigh = Math.max(...closes.slice(-30));
    if (currentPrice < recentLow * 1.01) buyScore += 15;
    if (currentPrice > recentHigh * 0.99) sellScore += 15;
    const totalScore = buyScore + sellScore;
    const confidence = Math.min(100, Math.round(totalScore * 1.1));
    if (buyScore > sellScore && confidence > 40) return { signal: 'BUY', confidence };
    if (sellScore > buyScore && confidence > 40) return { signal: 'SELL', confidence };
    return { signal: 'NEUTRE', confidence: Math.round(confidence * 0.5) };
}

function updateCandlesFromTick(symbol, price, timestamp) {
    TIMEFRAMES.forEach(tf => {
        const ms = TIMEFRAMES_MS[tf];
        const candleArray = candles[symbol][tf];
        const last = candleArray[candleArray.length - 1];
        const alignedTs = timestamp - (timestamp % ms);
        if (!last || alignedTs > last.timestamp) {
            candleArray.push({ timestamp: alignedTs, open: price, high: price, low: price, close: price, volume: 1 });
            if (candleArray.length > 300) candleArray.shift();
        } else {
            last.high = Math.max(last.high, price);
            last.low = Math.min(last.low, price);
            last.close = price;
            last.volume++;
        }
    });
}

// Connexion à Binance
function connectToBinance() {
    const streams = Object.values(ASSET_MAP).map(s => s.toLowerCase() + '@ticker').join('/');
    const wsUrl = 'wss://stream.binance.com:9443/stream?streams=' + streams;
    console.log('[Binance] Connexion à', wsUrl);
    const ws = new WebSocket(wsUrl);
    ws.on('open', () => {
        console.log('[Binance] Connecté');
        connected.value = true;
    });
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (msg.data) {
                const binanceSymbol = msg.data.s;
                const price = parseFloat(msg.data.c);
                const poSymbol = Object.keys(ASSET_MAP).find(k => ASSET_MAP[k] === binanceSymbol);
                if (poSymbol) {
                    lastPrices[poSymbol] = price;
                    updateCandlesFromTick(poSymbol, price, Date.now());
                }
            }
        } catch (e) {}
    });
    ws.on('close', () => {
        connected.value = false;
        console.log('[Binance] Déconnecté, reconnexion dans 5s');
        setTimeout(connectToBinance, 5000);
    });
    ws.on('error', (err) => console.error('[Binance] Erreur', err.message));
}

// Démarrage du serveur Express
const app = express();

// CORS obligatoire
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    next();
});

// Route API
app.get('/api/signals', (req, res) => {
    const tf = req.query.tf || '60s';
    if (!TIMEFRAMES.includes(tf)) return res.status(400).json({ error: 'Timeframe invalide' });
    const results = ASSETS.map(symbol => {
        const { signal, confidence } = getTradeSignal(symbol, tf);
        return {
            symbol: symbol.replace('_otc', ''),
            price: lastPrices[symbol] || 0,
            timeframe: tf,
            signal,
            confidence,
            connected: connected.value
        };
    });
    res.json(results);
});

// Lancer la connexion Binance et le serveur
connectToBinance();
const server = http.createServer(app);
server.listen(PORT, () => console.log(`Serveur prêt sur le port ${PORT}`));
