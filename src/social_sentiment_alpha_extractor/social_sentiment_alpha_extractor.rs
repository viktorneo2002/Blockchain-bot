use anyhow::{Result, Context, bail};
use async_trait::async_trait;
use backoff::{ExponentialBackoff, future::retry};
use chrono::{DateTime, Utc, Duration};
use dashmap::DashMap;
use futures::{StreamExt, SinkExt};
use log::{error, info, warn, debug};
use reqwest::{Client, header::{HeaderMap, HeaderValue, AUTHORIZATION}};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Sha256, Digest};
use std::collections::{HashMap, VecDeque, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Semaphore, mpsc, Mutex};
use tokio::time::{interval, sleep, timeout, Duration as TokioDuration};
use tokio_tungstenite::{connect_async, tungstenite::{Message, Error as WsError}};
use url::Url;

const SENTIMENT_WINDOW_SECS: i64 = 300;
const CACHE_TTL_SECS: u64 = 60;
const MAX_CONCURRENT_REQUESTS: usize = 30;
const SENTIMENT_THRESHOLD: f64 = 0.72;
const VOLUME_SPIKE_MULTIPLIER: f64 = 2.5;
const MIN_MENTIONS_THRESHOLD: u32 = 8;
const ALPHA_DECAY_FACTOR: f64 = 0.95;
const MAX_CACHE_SIZE: usize = 10000;
const DISCORD_HEARTBEAT_INTERVAL: u64 = 41250;
const WS_RECONNECT_DELAY_MS: u64 = 5000;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentSignal {
    pub token_address: String,
    pub symbol: String,
    pub sentiment_score: f64,
    pub volume_score: f64,
    pub momentum_score: f64,
    pub mentions_count: u32,
    pub unique_users: u32,
    pub timestamp: DateTime<Utc>,
    pub sources: Vec<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
struct TokenMetrics {
    mentions: Arc<Mutex<VecDeque<MentionData>>>,
    sentiment_history: Arc<Mutex<VecDeque<(DateTime<Utc>, f64)>>>,
    volume_history: Arc<Mutex<VecDeque<(DateTime<Utc>, u32)>>>,
    last_update: Arc<Mutex<DateTime<Utc>>>,
}

#[derive(Debug, Clone)]
struct MentionData {
    user_id: String,
    sentiment: f64,
    reach: u32,
    timestamp: DateTime<Utc>,
    source: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TwitterData {
    id: String,
    text: String,
    created_at: String,
    public_metrics: TwitterMetrics,
    author_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TwitterMetrics {
    retweet_count: u32,
    reply_count: u32,
    like_count: u32,
    quote_count: u32,
    impression_count: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct DiscordPayload {
    op: u8,
    d: Option<Value>,
    s: Option<u64>,
    t: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiscordIdentify {
    token: String,
    intents: u64,
    properties: HashMap<String, String>,
}

pub struct SocialSentimentExtractor {
    http_client: Client,
    token_metrics: Arc<DashMap<String, TokenMetrics>>,
    signal_cache: Arc<DashMap<String, (SentimentSignal, u64)>>,
    rate_limiter: Arc<Semaphore>,
    sentiment_analyzer: Arc<SentimentAnalyzer>,
    active_tokens: Arc<RwLock<HashMap<String, TokenInfo>>>,
    signal_sender: mpsc::Sender<SentimentSignal>,
    twitter_bearer: String,
    discord_token: Option<String>,
    running: Arc<AtomicBool>,
    discord_sequence: Arc<AtomicU64>,
}

#[derive(Clone)]
struct TokenInfo {
    address: String,
    symbol: String,
    keywords: Vec<String>,
}

struct SentimentAnalyzer {
    positive_patterns: Vec<(String, f64)>,
    negative_patterns: Vec<(String, f64)>,
    power_words: HashMap<String, f64>,
    scam_indicators: Vec<String>,
}

impl SentimentAnalyzer {
    fn new() -> Self {
        let mut positive_patterns = vec![
            ("moon", 1.2), ("pump", 1.0), ("bullish", 1.3), ("gem", 1.1),
            ("rocket", 1.2), ("100x", 1.5), ("explosive", 1.3), ("accumulate", 1.1),
            ("buy", 0.8), ("long", 0.9), ("hodl", 1.0), ("breakout", 1.2),
            ("parabolic", 1.4), ("alpha", 1.3), ("degen", 0.9), ("ape", 0.8),
            ("send", 0.7), ("lfg", 1.1), ("wagmi", 1.0), ("gm", 0.6),
            ("based", 0.8), ("chad", 0.9), ("gigachad", 1.1), ("whale alert", 1.3),
        ];

        let mut negative_patterns = vec![
            ("dump", 1.3), ("bearish", 1.2), ("scam", 2.0), ("rug", 2.5),
            ("sell", 0.9), ("short", 1.0), ("rekt", 1.5), ("exit", 1.1),
            ("dead", 1.4), ("crash", 1.3), ("ponzi", 2.0), ("honeypot", 2.5),
            ("avoid", 1.2), ("warning", 1.3), ("fake", 1.5), ("fraud", 2.0),
            ("stay away", 1.8), ("red flag", 1.6), ("ngmi", 1.1), ("cope", 0.8),
        ];

        let mut power_words = HashMap::new();
        power_words.insert("whale".to_string(), 1.3);
        power_words.insert("insider".to_string(), 1.4);
        power_words.insert("leaked".to_string(), 1.5);
        power_words.insert("confirmed".to_string(), 1.2);
        power_words.insert("announcement".to_string(), 1.3);
        power_words.insert("partnership".to_string(), 1.4);
        power_words.insert("listing".to_string(), 1.5);
        power_words.insert("mainnet".to_string(), 1.2);
        power_words.insert("airdrop".to_string(), 1.3);
        power_words.insert("burn".to_string(), 1.2);

        Self {
            positive_patterns: positive_patterns.into_iter()
                .map(|(s, w)| (s.to_string(), w))
                .collect(),
            negative_patterns: negative_patterns.into_iter()
                .map(|(s, w)| (s.to_string(), w))
                .collect(),
            power_words,
            scam_indicators: vec![
                "guaranteed returns", "risk free", "double your", "send sol to",
                "limited time only", "act now", "dm for details", "telegram admin",
                "whatsapp me", "private sale", "click this link", "connect wallet"
            ].iter().map(|s| s.to_string()).collect(),
        }
    }

    fn analyze(&self, text: &str) -> (f64, bool) {
        let lower_text = text.to_lowercase();
        let words: Vec<&str> = lower_text.split_whitespace().collect();
        let word_count = words.len() as f64;
        
        if word_count == 0.0 {
            return (0.5, false);
        }

        let mut positive_score = 0.0;
        let mut negative_score = 0.0;
        let mut power_modifier = 1.0;
        let mut is_scam = false;

        for (pattern, weight) in &self.positive_patterns {
            if lower_text.contains(pattern) {
                positive_score += weight;
            }
        }

        for (pattern, weight) in &self.negative_patterns {
            if lower_text.contains(pattern) {
                negative_score += weight;
            }
        }

        for (word, modifier) in &self.power_words {
            if lower_text.contains(word) {
                power_modifier *= modifier;
            }
        }

        for indicator in &self.scam_indicators {
            if lower_text.contains(indicator) {
                is_scam = true;
                negative_score += 3.0;
            }
        }

        let emoji_boost = self.count_bullish_emojis(&text) as f64 * 0.1;
        positive_score += emoji_boost;

        let raw_sentiment = (positive_score - negative_score) / word_count.sqrt();
        let normalized_sentiment = (raw_sentiment.tanh() + 1.0) / 2.0;
        let final_sentiment = (normalized_sentiment * power_modifier).clamp(0.0, 1.0);

        (final_sentiment, is_scam)
    }

    fn count_bullish_emojis(&self, text: &str) -> usize {
        let bullish_emojis = ["🚀", "🌙", "💎", "🔥", "📈", "💪", "🐂", "🟢"];
        bullish_emojis.iter().filter(|e| text.contains(**e)).count()
    }
}

impl SocialSentimentExtractor {
    pub async fn new(signal_sender: mpsc::Sender<SentimentSignal>) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert("User-Agent", HeaderValue::from_static("SolanaAlphaBot/1.0"));
        
        let http_client = Client::builder()
            .default_headers(headers)
            .timeout(TokioDuration::from_secs(10))
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(TokioDuration::from_secs(30))
            .build()
            .context("Failed to build HTTP client")?;

        let twitter_bearer = std::env::var("TWITTER_BEARER_TOKEN")
            .context("TWITTER_BEARER_TOKEN not set")?;
        
        let discord_token = std::env::var("DISCORD_BOT_TOKEN").ok();

        Ok(Self {
            http_client,
            token_metrics: Arc::new(DashMap::new()),
            signal_cache: Arc::new(DashMap::new()),
            rate_limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            sentiment_analyzer: Arc::new(SentimentAnalyzer::new()),
            active_tokens: Arc::new(RwLock::new(HashMap::new())),
            signal_sender,
            twitter_bearer,
            discord_token,
            running: Arc::new(AtomicBool::new(true)),
            discord_sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        
        let mut tasks = vec![
            tokio::spawn(self.clone().monitor_twitter_with_retry()),
            tokio::spawn(self.clone().process_sentiment_signals()),
            tokio::spawn(self.clone().cleanup_stale_data()),
        ];

        if self.discord_token.is_some() {
            tasks.push(tokio::spawn(self.clone().monitor_discord_with_retry()));
        }

        futures::future::try_join_all(tasks).await?;
        Ok(())
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub async fn add_token(&self, address: String, symbol: String) -> Result<()> {
        let keywords = self.generate_keywords(&symbol);
        let token_info = TokenInfo {
            address: address.clone(),
            symbol: symbol.clone(),
            keywords,
        };

        self.active_tokens.write().await.insert(address.clone(), token_info);
        self.token_metrics.insert(address.clone(), TokenMetrics {
            mentions: Arc::new(Mutex::new(VecDeque::new())),
            sentiment_history: Arc::new(Mutex::new(VecDeque::new())),
            volume_history: Arc::new(Mutex::new(VecDeque::new())),
            last_update: Arc::new(Mutex::new(Utc::now())),
        });

        info!("Added token tracking: {} ({})", symbol, address);
        Ok(())
    }

    fn generate_keywords(&self, symbol: &str) -> Vec<String> {
        let mut keywords = vec![
            symbol.to_lowercase(),
            format!("${}", symbol.to_lowercase()),
            format!("#{}", symbol.to_lowercase()),
        ];

        if symbol.len() <= 5 {
            keywords.push(symbol.to_uppercase());
            keywords.push(format!("${}", symbol.to_uppercase()));
        }

        keywords
    }

    async fn monitor_twitter_with_retry(self) -> Result<()> {
        let mut reconnect_attempts = 0;

        while self.running.load(Ordering::SeqCst) {
            match self.monitor_twitter_internal().await {
                Ok(_) => {
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("Twitter monitor error: {}", e);
                    reconnect_attempts += 1;
                    
                    if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                        bail!("Max reconnection attempts reached for Twitter monitor");
                    }
                    
                                        let delay = std::cmp::min(1000 * (1 << reconnect_attempts), 60000);
                    sleep(TokioDuration::from_millis(delay)).await;
                }
            }
        }
        Ok(())
    }

    async fn monitor_twitter_internal(&self) -> Result<()> {
        let mut interval = interval(TokioDuration::from_secs(2));
        
        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let tokens = self.active_tokens.read().await.clone();
            for (address, token_info) in tokens {
                if let Err(e) = self.fetch_twitter_mentions(&address, &token_info).await {
                    warn!("Twitter fetch error for {}: {}", token_info.symbol, e);
                }
            }
        }
        Ok(())
    }

    async fn fetch_twitter_mentions(&self, address: &str, token_info: &TokenInfo) -> Result<()> {
        let _permit = self.rate_limiter.acquire().await?;
        
        let query = token_info.keywords.iter()
            .map(|k| format!("\"{}\"", k))
            .collect::<Vec<_>>()
            .join(" OR ");
        
        let url = format!(
            "https://api.twitter.com/2/tweets/search/recent?query={}&max_results=100&tweet.fields=created_at,public_metrics,author_id&expansions=author_id&user.fields=public_metrics",
            urlencoding::encode(&query)
        );

        let backoff = ExponentialBackoff::default();
        let response = retry(backoff, || async {
            self.http_client
                .get(&url)
                .header(AUTHORIZATION, format!("Bearer {}", self.twitter_bearer))
                .send()
                .await
                .map_err(backoff::Error::transient)
        }).await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            bail!("Twitter API error: {} - {}", status, body);
        }

        let data: Value = response.json().await?;
        
        if let Some(tweets) = data["data"].as_array() {
            let users = data["includes"]["users"].as_array()
                .map(|u| u.iter()
                    .filter_map(|user| {
                        let id = user["id"].as_str()?;
                        let followers = user["public_metrics"]["followers_count"].as_u64()?;
                        Some((id.to_string(), followers))
                    })
                    .collect::<HashMap<_, _>>())
                .unwrap_or_default();

            for tweet_data in tweets {
                if let Ok(tweet) = serde_json::from_value::<TwitterData>(tweet_data.clone()) {
                    let follower_count = users.get(&tweet.author_id).copied().unwrap_or(0);
                    self.process_twitter_mention(address, token_info, tweet, follower_count).await?;
                }
            }
        }

        Ok(())
    }

    async fn process_twitter_mention(
        &self, 
        address: &str, 
        _token_info: &TokenInfo, 
        tweet: TwitterData,
        follower_count: u64
    ) -> Result<()> {
        let (sentiment, is_scam) = self.sentiment_analyzer.analyze(&tweet.text);
        
        if is_scam {
            return Ok(());
        }

        let engagement = tweet.public_metrics.like_count + 
                        tweet.public_metrics.retweet_count * 2 + 
                        tweet.public_metrics.reply_count +
                        tweet.public_metrics.quote_count * 3;

        let reach = engagement + (follower_count as u32 / 100).min(1000);

        let mention = MentionData {
            user_id: tweet.author_id,
            sentiment,
            reach,
            timestamp: DateTime::parse_from_rfc3339(&tweet.created_at)?.with_timezone(&Utc),
            source: "twitter".to_string(),
        };

        self.update_token_metrics(address, mention).await;
        Ok(())
    }

    async fn monitor_discord_with_retry(self) -> Result<()> {
        let token = self.discord_token.as_ref()
            .context("Discord token not available")?;
        
        let mut reconnect_attempts = 0;

        while self.running.load(Ordering::SeqCst) {
            match self.monitor_discord_internal(token).await {
                Ok(_) => {
                    info!("Discord monitor disconnected normally");
                    reconnect_attempts = 0;
                }
                Err(e) => {
                    error!("Discord monitor error: {}", e);
                    reconnect_attempts += 1;
                    
                    if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                        bail!("Max reconnection attempts reached for Discord monitor");
                    }
                    
                    let delay = std::cmp::min(1000 * (1 << reconnect_attempts), 60000);
                    sleep(TokioDuration::from_millis(delay)).await;
                }
            }
        }
        Ok(())
    }

    async fn monitor_discord_internal(&self, token: &str) -> Result<()> {
        let url = Url::parse("wss://gateway.discord.gg/?v=10&encoding=json")?;
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let heartbeat_interval = Arc::new(AtomicU64::new(0));
        let last_heartbeat_ack = Arc::new(AtomicBool::new(true));

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(payload) = serde_json::from_str::<DiscordPayload>(&text) {
                        match payload.op {
                            10 => {
                                let interval_ms = payload.d
                                    .and_then(|d| d["heartbeat_interval"].as_u64())
                                    .unwrap_or(DISCORD_HEARTBEAT_INTERVAL);
                                
                                heartbeat_interval.store(interval_ms, Ordering::SeqCst);
                                
                                let identify = json!({
                                    "op": 2,
                                    "d": {
                                        "token": token,
                                        "intents": 513,
                                        "properties": {
                                            "$os": "linux",
                                            "$browser": "solana-bot",
                                            "$device": "solana-bot"
                                        }
                                    }
                                });
                                
                                write.send(Message::Text(identify.to_string())).await?;
                                
                                let hb_interval = heartbeat_interval.clone();
                                let hb_ack = last_heartbeat_ack.clone();
                                let seq = self.discord_sequence.clone();
                                let running = self.running.clone();
                                
                                tokio::spawn(async move {
                                    let mut interval = interval(TokioDuration::from_millis(
                                        hb_interval.load(Ordering::SeqCst)
                                    ));
                                    
                                    while running.load(Ordering::SeqCst) {
                                        interval.tick().await;
                                        
                                        if !hb_ack.load(Ordering::SeqCst) {
                                            error!("Discord heartbeat ACK not received");
                                            break;
                                        }
                                        
                                        hb_ack.store(false, Ordering::SeqCst);
                                        
                                        let heartbeat = json!({
                                            "op": 1,
                                            "d": seq.load(Ordering::SeqCst)
                                        });
                                        
                                        if let Err(e) = write.send(Message::Text(heartbeat.to_string())).await {
                                            error!("Failed to send heartbeat: {}", e);
                                            break;
                                        }
                                    }
                                });
                            }
                            11 => {
                                last_heartbeat_ack.store(true, Ordering::SeqCst);
                            }
                            0 => {
                                if let Some(s) = payload.s {
                                    self.discord_sequence.store(s, Ordering::SeqCst);
                                }
                                
                                if payload.t.as_deref() == Some("MESSAGE_CREATE") {
                                    if let Some(data) = payload.d {
                                        self.process_discord_message(data).await?;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn process_discord_message(&self, message: Value) -> Result<()> {
        let content = message["content"].as_str().unwrap_or("");
        let author_id = message["author"]["id"].as_str().unwrap_or("");
        let channel_id = message["channel_id"].as_str().unwrap_or("");
        
        if content.is_empty() || author_id.is_empty() {
            return Ok(());
        }

        let tokens = self.active_tokens.read().await.clone();
        for (address, token_info) in tokens {
            if token_info.keywords.iter().any(|k| content.to_lowercase().contains(k)) {
                let (sentiment, is_scam) = self.sentiment_analyzer.analyze(content);
                
                if !is_scam {
                    let mention = MentionData {
                        user_id: format!("{}:{}", author_id, channel_id),
                        sentiment,
                        reach: 10,
                        timestamp: Utc::now(),
                        source: "discord".to_string(),
                    };
                    
                    self.update_token_metrics(&address, mention).await;
                }
            }
        }

        Ok(())
    }

    async fn update_token_metrics(&self, address: &str, mention: MentionData) {
        if let Some(metrics) = self.token_metrics.get(address) {
            let cutoff_time = Utc::now() - Duration::seconds(SENTIMENT_WINDOW_SECS);
            
            let mut mentions = metrics.mentions.lock().await;
            mentions.retain(|m| m.timestamp > cutoff_time);
            mentions.push_back(mention.clone());
            
            let mut sentiment_history = metrics.sentiment_history.lock().await;
            sentiment_history.retain(|(t, _)| *t > cutoff_time);
            
            let current_sentiment = self.calculate_weighted_sentiment(&mentions);
            sentiment_history.push_back((Utc::now(), current_sentiment));
            
            let mut volume_history = metrics.volume_history.lock().await;
            volume_history.retain(|(t, _)| *t > cutoff_time);
            
            let current_volume = mentions.len() as u32;
            volume_history.push_back((Utc::now(), current_volume));
            
            *metrics.last_update.lock().await = Utc::now();
        }
    }

    fn calculate_weighted_sentiment(&self, mentions: &VecDeque<MentionData>) -> f64 {
        if mentions.is_empty() {
            return 0.5;
        }

        let now = Utc::now();
        let mut weighted_sum = 0.0;
        let mut weight_total = 0.0;

        for mention in mentions {
            let age_seconds = (now - mention.timestamp).num_seconds() as f64;
            let time_weight = (-age_seconds / (SENTIMENT_WINDOW_SECS as f64)).exp();
            let reach_weight = (mention.reach as f64 + 1.0).ln();
            let total_weight = time_weight * reach_weight;
            
            weighted_sum += mention.sentiment * total_weight;
            weight_total += total_weight;
        }

        if weight_total > 0.0 {
            (weighted_sum / weight_total).clamp(0.0, 1.0)
        } else {
            0.5
        }
    }

    async fn process_sentiment_signals(self) -> Result<()> {
        let mut interval = interval(TokioDuration::from_secs(1));
        
        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let tokens = self.active_tokens.read().await.clone();
            for (address, token_info) in tokens {
                match timeout(
                    TokioDuration::from_millis(500),
                    self.generate_signal(&address, &token_info)
                ).await {
                    Ok(Some(signal)) if signal.confidence >= SENTIMENT_THRESHOLD => {
                        if let Err(e) = self.signal_sender.send(signal.clone()).await {
                            error!("Failed to send signal: {}", e);
                        }
                        
                        self.signal_cache.insert(
                            address.clone(),
                            (signal, SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
                        );
                    }
                    Err(_) => warn!("Signal generation timeout for {}", token_info.symbol),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn generate_signal(&self, address: &str, token_info: &TokenInfo) -> Option<SentimentSignal> {
        let metrics = self.token_metrics.get(address)?;
        
        let mentions = metrics.mentions.lock().await;
        if mentions.len() < MIN_MENTIONS_THRESHOLD as usize {
            return None;
        }

        let sentiment_history = metrics.sentiment_history.lock().await;
        let volume_history = metrics.volume_history.lock().await;

        let sentiment_score = self.calculate_weighted_sentiment(&mentions);
        let volume_score = self.calculate_volume_score(&volume_history);
        let momentum_score = self.calculate_momentum_score(&sentiment_history);
        
        let unique_users = mentions.iter()
            .map(|m| m.user_id.clone())
            .collect::<HashSet<_>>()
            .len() as u32;

        let sources = mentions.iter()
            .map(|m| m.source.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

                let confidence = self.calculate_confidence(
            sentiment_score,
            volume_score,
            momentum_score,
            unique_users,
            mentions.len() as u32
        );

        drop(mentions);
        drop(sentiment_history);
        drop(volume_history);

        Some(SentimentSignal {
            token_address: address.to_string(),
            symbol: token_info.symbol.clone(),
            sentiment_score,
            volume_score,
            momentum_score,
            mentions_count: self.token_metrics.get(address)?.mentions.lock().await.len() as u32,
            unique_users,
            timestamp: Utc::now(),
            sources,
            confidence,
        })
    }

    fn calculate_volume_score(&self, volume_history: &VecDeque<(DateTime<Utc>, u32)>) -> f64 {
        if volume_history.len() < 10 {
            return 0.0;
        }

        let recent_volumes: Vec<u32> = volume_history.iter()
            .rev()
            .take(20)
            .map(|(_, v)| *v)
            .collect();

        if recent_volumes.is_empty() {
            return 0.0;
        }

        let mean = recent_volumes.iter().sum::<u32>() as f64 / recent_volumes.len() as f64;
        if mean == 0.0 {
            return 0.0;
        }

        let variance = recent_volumes.iter()
            .map(|v| (*v as f64 - mean).powi(2))
            .sum::<f64>() / recent_volumes.len() as f64;
        
        let std_dev = variance.sqrt();
        let latest_volume = recent_volumes[0] as f64;

        if std_dev > 0.0 {
            let z_score = (latest_volume - mean) / std_dev;
            if z_score >= 2.0 {
                1.0
            } else if z_score >= 1.0 {
                0.5 + (z_score - 1.0) * 0.5
            } else {
                (z_score / 2.0).max(0.0)
            }
        } else if latest_volume >= mean * VOLUME_SPIKE_MULTIPLIER {
            1.0
        } else {
            (latest_volume / (mean * VOLUME_SPIKE_MULTIPLIER)).min(1.0)
        }
    }

    fn calculate_momentum_score(&self, sentiment_history: &VecDeque<(DateTime<Utc>, f64)>) -> f64 {
        if sentiment_history.len() < 5 {
            return 0.0;
        }

        let recent_sentiments: Vec<f64> = sentiment_history.iter()
            .rev()
            .take(30)
            .map(|(_, s)| *s)
            .collect();

        if recent_sentiments.len() < 2 {
            return 0.0;
        }

        let mut weighted_momentum = 0.0;
        let mut weight_sum = 0.0;
        let mut weight = 1.0;

        for i in 1..recent_sentiments.len().min(20) {
            let change = recent_sentiments[i-1] - recent_sentiments[i];
            weighted_momentum += change * weight;
            weight_sum += weight;
            weight *= ALPHA_DECAY_FACTOR;
        }

        if weight_sum > 0.0 {
            let normalized_momentum = weighted_momentum / weight_sum;
            normalized_momentum.tanh()
        } else {
            0.0
        }
    }

    fn calculate_confidence(
        &self,
        sentiment_score: f64,
        volume_score: f64,
        momentum_score: f64,
        unique_users: u32,
        mentions_count: u32
    ) -> f64 {
        const SENTIMENT_WEIGHT: f64 = 0.3;
        const VOLUME_WEIGHT: f64 = 0.25;
        const MOMENTUM_WEIGHT: f64 = 0.25;
        const DIVERSITY_WEIGHT: f64 = 0.2;

        let diversity_score = if mentions_count > 0 {
            (unique_users as f64 / mentions_count as f64).powf(0.8)
        } else {
            0.0
        };
        
        let normalized_momentum = (momentum_score + 1.0) / 2.0;
        
        let weighted_score = sentiment_score * SENTIMENT_WEIGHT +
                           volume_score * VOLUME_WEIGHT +
                           normalized_momentum * MOMENTUM_WEIGHT +
                           diversity_score * DIVERSITY_WEIGHT;

        let mention_factor = if mentions_count >= MIN_MENTIONS_THRESHOLD {
            let ratio = mentions_count as f64 / MIN_MENTIONS_THRESHOLD as f64;
            1.0 + (ratio.ln() * 0.1).min(0.5)
        } else {
            0.5
        };

        (weighted_score * mention_factor).clamp(0.0, 1.0)
    }

    async fn cleanup_stale_data(self) -> Result<()> {
        let mut interval = interval(TokioDuration::from_secs(30));
        
        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let cache_cutoff = now - CACHE_TTL_SECS;
            
            let mut entries_to_remove = Vec::new();
            for entry in self.signal_cache.iter() {
                if entry.value().1 <= cache_cutoff {
                    entries_to_remove.push(entry.key().clone());
                }
            }
            
            for key in entries_to_remove {
                self.signal_cache.remove(&key);
            }
            
            if self.signal_cache.len() > MAX_CACHE_SIZE {
                let mut entries: Vec<(String, u64)> = self.signal_cache.iter()
                    .map(|entry| (entry.key().clone(), entry.value().1))
                    .collect();
                
                entries.sort_unstable_by_key(|e| e.1);
                
                let remove_count = self.signal_cache.len().saturating_sub(MAX_CACHE_SIZE / 2);
                for (key, _) in entries.into_iter().take(remove_count) {
                    self.signal_cache.remove(&key);
                }
            }
            
            let cutoff_time = Utc::now() - Duration::seconds(SENTIMENT_WINDOW_SECS * 2);
            for entry in self.token_metrics.iter() {
                let mut mentions = entry.mentions.lock().await;
                mentions.retain(|m| m.timestamp > cutoff_time);
                drop(mentions);
                
                let mut sentiment_history = entry.sentiment_history.lock().await;
                sentiment_history.retain(|(t, _)| *t > cutoff_time);
                drop(sentiment_history);
                
                let mut volume_history = entry.volume_history.lock().await;
                volume_history.retain(|(t, _)| *t > cutoff_time);
            }
        }
        Ok(())
    }

    pub async fn get_top_signals(&self, limit: usize) -> Vec<SentimentSignal> {
        let mut signals: Vec<(SentimentSignal, u64)> = self.signal_cache.iter()
            .map(|entry| (entry.value().0.clone(), entry.value().1))
            .collect();
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        signals.retain(|(_, timestamp)| now - timestamp < CACHE_TTL_SECS);
        
        signals.sort_unstable_by(|a, b| {
            b.0.confidence.partial_cmp(&a.0.confidence).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        signals.truncate(limit);
        signals.into_iter().map(|(signal, _)| signal).collect()
    }

    pub async fn get_signal(&self, token_address: &str) -> Option<SentimentSignal> {
        self.signal_cache.get(token_address)
            .filter(|entry| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                now - entry.value().1 < CACHE_TTL_SECS
            })
            .map(|entry| entry.value().0.clone())
    }

    pub async fn remove_token(&self, address: &str) -> Result<()> {
        self.active_tokens.write().await.remove(address);
        self.token_metrics.remove(address);
        self.signal_cache.remove(address);
        info!("Removed token tracking: {}", address);
        Ok(())
    }

    pub fn get_metrics_summary(&self, address: &str) -> Option<HashMap<String, Value>> {
        let metrics = self.token_metrics.get(address)?;
        
        tokio::task::block_in_place(|| {
            let mentions = futures::executor::block_on(metrics.mentions.lock());
            let sentiment_history = futures::executor::block_on(metrics.sentiment_history.lock());
            let volume_history = futures::executor::block_on(metrics.volume_history.lock());
            let last_update = futures::executor::block_on(metrics.last_update.lock());
            
            let mut summary = HashMap::new();
            
            summary.insert("mentions_count".to_string(), 
                json!(mentions.len()));
            
            let unique_users = mentions.iter()
                .map(|m| m.user_id.clone())
                .collect::<HashSet<_>>()
                .len();
            summary.insert("unique_users".to_string(), 
                json!(unique_users));
            
            let avg_sentiment = self.calculate_weighted_sentiment(&mentions);
            summary.insert("average_sentiment".to_string(),
                json!(avg_sentiment));
            
            summary.insert("last_update".to_string(),
                json!(last_update.to_rfc3339()));
            
            if !sentiment_history.is_empty() {
                let latest_sentiment = sentiment_history.back().map(|(_, s)| *s).unwrap_or(0.5);
                summary.insert("latest_sentiment".to_string(), json!(latest_sentiment));
            }
            
            if !volume_history.is_empty() {
                let latest_volume = volume_history.back().map(|(_, v)| *v).unwrap_or(0);
                summary.insert("latest_volume".to_string(), json!(latest_volume));
            }
            
            Some(summary)
        })
    }
}

impl Clone for SocialSentimentExtractor {
    fn clone(&self) -> Self {
        Self {
            http_client: self.http_client.clone(),
            token_metrics: self.token_metrics.clone(),
            signal_cache: self.signal_cache.clone(),
            rate_limiter: self.rate_limiter.clone(),
            sentiment_analyzer: self.sentiment_analyzer.clone(),
            active_tokens: self.active_tokens.clone(),
            signal_sender: self.signal_sender.clone(),
            twitter_bearer: self.twitter_bearer.clone(),
            discord_token: self.discord_token.clone(),
            running: self.running.clone(),
            discord_sequence: self.discord_sequence.clone(),
        }
    }
}

#[async_trait]
pub trait SentimentExtractor: Send + Sync {
    async fn extract_signals(&self) -> Result<Vec<SentimentSignal>>;
    async fn track_token(&self, address: String, symbol: String) -> Result<()>;
    async fn untrack_token(&self, address: &str) -> Result<()>;
}

#[async_trait]
impl SentimentExtractor for SocialSentimentExtractor {
    async fn extract_signals(&self) -> Result<Vec<SentimentSignal>> {
        Ok(self.get_top_signals(50).await)
    }

    async fn track_token(&self, address: String, symbol: String) -> Result<()> {
        self.add_token(address, symbol).await
    }

    async fn untrack_token(&self, address: &str) -> Result<()> {
        self.remove_token(address).await
    }
}

