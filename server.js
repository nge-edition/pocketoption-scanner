const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

// ── Configuration ─────────────────────────────────
const PORT = process.env.PORT || 3000;

// Endpoints WebSocket PocketOption (région démo Europe)
const PO_DEMO_URL = 'wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket';
const PO_BACKUP_URL = 'wss://try-demo-eu.po.market/socket.io/?EIO=4&transport=websocket';

// Timeframes PocketOption (en secondes) → équivalence en millisecondes
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

// Actifs OTC à suivre (principales paires forex/crypto)
const ASSETS = [
    'EURUSD_otc', 'GBPUSD_otc', 'USDJPY_otc', 'AUDUSD_otc', 'USDCAD_otc',
    'BTCUSD_otc', 'ETHUSD_otc', 'EURGBP_otc', 'GBPJPY_otc', 'EURJPY_otc'
];

// ── Stockage des données en mémoire ───────────────
const candles = {};    // { symbol: { timeframe: [candle, ...] } }
const lastPrices = {}; // { symbol: prix actuel }
const connected = { value: false };

ASSETS.forEach(s => {
    candles[s] = {};
    TIMEFRAMES.forEach(tf => candles[s][tf] = []);
    lastPrices[s] = 0;
});

// ── Fonctions de calcul des indicateurs ──────────
function calculateRSI(prices, period = 14) {
    if (prices.length < period + 1) return 50;
    let gains = 0, losses = 0;
    for (let i = prices.length - period; i < prices.length; i++) {
        const diff = prices[i] - prices[i - 1];
        if (diff >= 0) gains += diff;
        else losses -= diff;
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
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

// ── Génération du signal ─────────────────────────
function getTradeSignal(symbol, timeframe) {
    const closes = candles[symbol][timeframe].map(c => c.close);
    if (closes.length < 50) return { signal: 'NEUTRE', confidence: 0 };

    const currentPrice = closes[closes.length - 1];
    const rsi = calculateRSI(closes, 14);
    const ema20 = calculateEMA(closes, 20);
    const ema50 = calculateEMA(closes, 50);
    const { macd, signal: macdSignal, histogram } = calculateMACD(closes);

    let buyScore = 0, sellScore = 0;

    if (rsi < 30) buyScore += 25;
    else if (rsi > 70) sellScore += 25;

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

// ── Mise à jour des chandelles ───────────────────
function updateCandlesFromTick(symbol, price, timestamp) {
    TIMEFRAMES.forEach(tf => {
        const ms = TIMEFRAMES_MS[tf];
        const candleArray = candles[symbol][tf];
        const lastCandle = candleArray[candleArray.length - 1];
        const alignedTs = timestamp - (timestamp % ms);

        if (!lastCandle || alignedTs > lastCandle.timestamp) {
            candleArray.push({
                timestamp: alignedTs,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1
            });
            if (candleArray.length > 300) candleArray.shift();
        } else {
            lastCandle.high = Math.max(lastCandle.high, price);
            lastCandle.low = Math.min(lastCandle.low, price);
            lastCandle.close = price;
            lastCandle.volume++;
        }
    });
}

// ── Connexion WebSocket PocketOption ──────────────
function connectToPocketOption() {
    console.log(`[PO] Tentative de connexion à : ${PO_DEMO_URL}`);
    
    // Créer un WebSocket standard (sans socket.io, on parle le protocole Engine.IO 4)
    const ws = new WebSocket(PO_DEMO_URL, {
        headers: {
            'Origin': 'https://pocketoption.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    });

    ws.on('open', () => {
        console.log('[PO] Connecté au WebSocket PocketOption');
        connected.value = true;
        // Le protocole Engine.IO demande d'envoyer "40" pour initier la connexion Socket.IO
        ws.send('40');
    });

    ws.on('message', (data) => {
        const msg = data.toString();
        
        // Engine.IO ping-pong
        if (msg === '2') { ws.send('3'); return; } // Pong
        if (msg === '3') return; // Ping reçu, déjà répondu
        
        // Message Socket.IO (format: 42["event", {...}])
        if (msg.startsWith('42')) {
            try {
                const jsonStr = msg.substring(2);
                const [event, payload] = JSON.parse(jsonStr);
                
                // Réception des ticks de prix
                if (event === 'update_close_value' && payload) {
                    const assets = Array.isArray(payload) ? payload : [payload];
                    assets.forEach(item => {
                        if (item.asset && item.price) {
                            const symbol = item.asset;
                            const price = parseFloat(item.price);
                            const ts = Date.now();
                            
                            lastPrices[symbol] = price;
                            updateCandlesFromTick(symbol, price, ts);
                        }
                    });
                }
            } catch (e) {
                // Ignore les messages mal formés
            }
        }
    });

    ws.on('close', () => {
        console.log('[PO] Connexion fermée, reconnexion dans 5s...');
        connected.value = false;
        setTimeout(connectToPocketOption, 5000);
    });

    ws.on('error', (err) => {
        console.error('[PO] Erreur WebSocket:', err.message);
    });
}

// ── Serveur Express ─────────────────────────────
const app = express();
// CORS pour permettre les requêtes depuis Hostinger
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    next();
});
app.use(express.static(path.join(__dirname, 'public')));

// API pour obtenir les signaux
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

// API pour obtenir la liste des timeframes disponibles
app.get('/api/timeframes', (req, res) => {
    res.json(TIMEFRAMES);
});

// ── Démarrage ────────────────────────────────────
connectToPocketOption();
const server = http.createServer(app);
server.listen(PORT, () => {
    console.log(`🚀 Scanner PocketOption actif sur http://localhost:${PORT}`);
});

// Garder le processus en vie
process.on('uncaughtException', (err) => console.error('Erreur non capturée:', err));
