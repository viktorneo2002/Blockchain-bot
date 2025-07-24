"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createConfigFromEnv = exports.validateConfig = exports.getConfigManager = exports.ConfigManager = void 0;
const web3_js_1 = require("@solana/web3.js");
const fs_1 = require("fs");
const path_1 = require("path");
const crypto_1 = require("crypto");
const dotenv = __importStar(require("dotenv"));
const joi_1 = __importDefault(require("joi"));
const winston_1 = __importDefault(require("winston"));
const events_1 = require("events");
dotenv.config();
const CONFIG_SCHEMA = joi_1.default.object({
    rpc: joi_1.default.object({
        primary: joi_1.default.string().uri().required(),
        fallback: joi_1.default.array().items(joi_1.default.string().uri()).min(1).required(),
        commitment: joi_1.default.string().valid('processed', 'confirmed', 'finalized').required(),
        confirmTransactionInitialTimeout: joi_1.default.number().min(10000).max(120000).required(),
        httpHeaders: joi_1.default.object().pattern(joi_1.default.string(), joi_1.default.string()).optional(),
        wsEndpoint: joi_1.default.string().uri().optional(),
        disableRetryOnRateLimit: joi_1.default.boolean().required()
    }).required(),
    wallet: joi_1.default.object({
        privateKey: joi_1.default.string().required(),
        publicKey: joi_1.default.string().required(),
        maxConcurrentTransactions: joi_1.default.number().min(1).max(50).required(),
        priorityFeeMode: joi_1.default.string().valid('dynamic', 'fixed', 'percentile').required(),
        priorityFeeValue: joi_1.default.number().min(0).required(),
        computeUnitLimit: joi_1.default.number().min(200000).max(1400000).required()
    }).required(),
    trading: joi_1.default.object({
        maxPositionSize: joi_1.default.number().positive().required(),
        minPositionSize: joi_1.default.number().positive().required(),
        maxSlippageBps: joi_1.default.number().min(0).max(10000).required(),
        defaultSlippageBps: joi_1.default.number().min(0).max(10000).required(),
        maxGasPrice: joi_1.default.number().positive().required(),
        profitTargetPercent: joi_1.default.number().positive().required(),
        stopLossPercent: joi_1.default.number().positive().required(),
        trailingStopPercent: joi_1.default.number().positive().required(),
        maxOpenPositions: joi_1.default.number().min(1).max(100).required(),
        cooldownPeriodMs: joi_1.default.number().min(0).required(),
        blacklistTokens: joi_1.default.array().items(joi_1.default.string()).required(),
        whitelistTokens: joi_1.default.array().items(joi_1.default.string()).required(),
        tradingEnabled: joi_1.default.boolean().required()
    }).required(),
    strategy: joi_1.default.object({
        type: joi_1.default.string().valid('arbitrage', 'sniper', 'sandwich', 'liquidation', 'market-making').required(),
        parameters: joi_1.default.object().required(),
        riskLevel: joi_1.default.string().valid('conservative', 'moderate', 'aggressive').required(),
        rebalanceIntervalMs: joi_1.default.number().min(1000).required(),
        minProfitThreshold: joi_1.default.number().min(0).required(),
        emergencyExitEnabled: joi_1.default.boolean().required()
    }).required(),
    monitoring: joi_1.default.object({
        metricsEnabled: joi_1.default.boolean().required(),
        metricsPort: joi_1.default.number().port().required(),
        alertWebhook: joi_1.default.string().uri().optional(),
        performanceTrackingInterval: joi_1.default.number().min(1000).required(),
        healthCheckInterval: joi_1.default.number().min(1000).required(),
        logLevel: joi_1.default.string().valid('error', 'warn', 'info', 'debug').required(),
        logRetentionDays: joi_1.default.number().min(1).max(365).required()
    }).required(),
    security: joi_1.default.object({
        encryptionEnabled: joi_1.default.boolean().required(),
        encryptionKey: joi_1.default.string().when('encryptionEnabled', { is: true, then: joi_1.default.required() }).optional(),
        ipWhitelist: joi_1.default.array().items(joi_1.default.string().ip()).required(),
        maxRequestsPerMinute: joi_1.default.number().min(1).required(),
        autoLockAfterInactivity: joi_1.default.number().min(0).required(),
        requireMFA: joi_1.default.boolean().required()
    }).required(),
    network: joi_1.default.object({
        chainId: joi_1.default.string().valid('mainnet-beta', 'devnet', 'testnet').required(),
        programIds: joi_1.default.object().pattern(joi_1.default.string(), joi_1.default.string()).required(),
        tokenListUrl: joi_1.default.string().uri().required(),
        priceOracleEndpoints: joi_1.default.array().items(joi_1.default.string().uri()).min(1).required(),
        blockExplorerUrl: joi_1.default.string().uri().required()
    }).required(),
    version: joi_1.default.string().required(),
    lastUpdated: joi_1.default.number().required()
});
class ConfigManager extends events_1.EventEmitter {
    constructor() {
        super();
        this.connections = new Map();
        this.configPath = process.env.CONFIG_PATH || (0, path_1.join)(process.cwd(), 'config', 'bot-config.json');
        this.logger = this.initializeLogger();
        this.config = this.loadConfig();
        this.configHash = this.calculateConfigHash();
        this.initializeConnections();
        this.startConfigWatcher();
    }
    static getInstance() {
        if (!ConfigManager.instance) {
            ConfigManager.instance = new ConfigManager();
        }
        return ConfigManager.instance;
    }
    initializeLogger() {
        const logDir = (0, path_1.join)(process.cwd(), 'logs');
        if (!(0, fs_1.existsSync)(logDir)) {
            (0, fs_1.mkdirSync)(logDir, { recursive: true });
        }
        return winston_1.default.createLogger({
            level: process.env.LOG_LEVEL || 'info',
            format: winston_1.default.format.combine(winston_1.default.format.timestamp(), winston_1.default.format.errors({ stack: true }), winston_1.default.format.json()),
            transports: [
                new winston_1.default.transports.File({
                    filename: (0, path_1.join)(logDir, 'config-error.log'),
                    level: 'error'
                }),
                new winston_1.default.transports.File({
                    filename: (0, path_1.join)(logDir, 'config-combined.log')
                }),
                new winston_1.default.transports.Console({
                    format: winston_1.default.format.combine(winston_1.default.format.colorize(), winston_1.default.format.simple())
                })
            ]
        });
    }
    loadConfig() {
        try {
            const configDir = (0, path_1.dirname)(this.configPath);
            if (!(0, fs_1.existsSync)(configDir)) {
                (0, fs_1.mkdirSync)(configDir, { recursive: true });
            }
            if (!(0, fs_1.existsSync)(this.configPath)) {
                const defaultConfig = this.createDefaultConfig();
                this.saveConfig(defaultConfig);
                return defaultConfig;
            }
            const configData = (0, fs_1.readFileSync)(this.configPath, 'utf-8');
            const parsedConfig = JSON.parse(configData);
            const { error, value } = CONFIG_SCHEMA.validate(parsedConfig, {
                abortEarly: false,
                stripUnknown: true
            });
            if (error) {
                this.logger.error('Config validation failed', { errors: error.details });
                throw new Error(`Invalid configuration: ${error.message}`);
            }
            this.logger.info('Configuration loaded successfully');
            return value;
        }
        catch (error) {
            this.logger.error('Failed to load configuration', { error });
            throw error;
        }
    }
    createDefaultConfig() {
        return {
            rpc: {
                primary: process.env.RPC_PRIMARY || 'https://api.mainnet-beta.solana.com',
                fallback: [
                    process.env.RPC_FALLBACK_1 || 'https://solana-api.projectserum.com',
                    process.env.RPC_FALLBACK_2 || 'https://rpc.ankr.com/solana'
                ],
                commitment: 'confirmed',
                confirmTransactionInitialTimeout: 60000,
                httpHeaders: {
                    'Cache-Control': 'no-cache',
                    'X-Bot-Version': '1.0.0'
                },
                wsEndpoint: process.env.WS_ENDPOINT,
                disableRetryOnRateLimit: false
            },
            wallet: {
                privateKey: process.env.WALLET_PRIVATE_KEY || '',
                publicKey: process.env.WALLET_PUBLIC_KEY || '',
                maxConcurrentTransactions: 10,
                priorityFeeMode: 'dynamic',
                priorityFeeValue: 1000,
                computeUnitLimit: 400000
            },
            trading: {
                maxPositionSize: 100,
                minPositionSize: 0.1,
                maxSlippageBps: 300,
                defaultSlippageBps: 100,
                maxGasPrice: 0.01,
                profitTargetPercent: 2,
                stopLossPercent: 1,
                trailingStopPercent: 0.5,
                maxOpenPositions: 5,
                cooldownPeriodMs: 5000,
                blacklistTokens: [],
                whitelistTokens: [],
                tradingEnabled: true
            },
            strategy: {
                type: 'arbitrage',
                parameters: {
                    minSpreadPercent: 0.5,
                    maxLatencyMs: 100,
                    orderBookDepth: 10,
                    executionMode: 'aggressive'
                },
                riskLevel: 'moderate',
                rebalanceIntervalMs: 300000,
                minProfitThreshold: 0.1,
                emergencyExitEnabled: true
            },
            monitoring: {
                metricsEnabled: true,
                metricsPort: 9090,
                alertWebhook: process.env.ALERT_WEBHOOK,
                performanceTrackingInterval: 60000,
                healthCheckInterval: 30000,
                logLevel: 'info',
                logRetentionDays: 30
            },
            security: {
                encryptionEnabled: true,
                encryptionKey: process.env.ENCRYPTION_KEY,
                ipWhitelist: ['127.0.0.1'],
                maxRequestsPerMinute: 1000,
                autoLockAfterInactivity: 3600000,
                requireMFA: false
            },
            network: {
                chainId: 'mainnet-beta',
                programIds: {
                    'TOKEN_PROGRAM': 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                    'SERUM_DEX': '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin',
                    'RAYDIUM_AMM': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
                },
                tokenListUrl: 'https://token.jup.ag/all',
                priceOracleEndpoints: [
                    'https://price.jup.ag/v4/price',
                    'https://api.coingecko.com/api/v3'
                ],
                blockExplorerUrl: 'https://explorer.solana.com'
            },
            version: '1.0.0',
            lastUpdated: Date.now()
        };
    }
    saveConfig(config) {
        try {
            const configData = JSON.stringify(config, null, 2);
            (0, fs_1.writeFileSync)(this.configPath, configData, 'utf-8');
            this.logger.info('Configuration saved successfully');
        }
        catch (error) {
            this.logger.error('Failed to save configuration', { error });
            throw error;
        }
    }
    calculateConfigHash() {
        const configString = JSON.stringify(this.config);
        return (0, crypto_1.createHash)('sha256').update(configString).digest('hex');
    }
    initializeConnections() {
        try {
            const primaryConnection = new web3_js_1.Connection(this.config.rpc.primary, {
                commitment: this.config.rpc.commitment,
                confirmTransactionInitialTimeout: this.config.rpc.confirmTransactionInitialTimeout,
                httpHeaders: this.config.rpc.httpHeaders,
                disableRetryOnRateLimit: this.config.rpc.disableRetryOnRateLimit
            });
            this.connections.set('primary', primaryConnection);
            this.config.rpc.fallback.forEach((endpoint, index) => {
                const connection = new web3_js_1.Connection(endpoint, {
                    commitment: this.config.rpc.commitment,
                    confirmTransactionInitialTimeout: this.config.rpc.confirmTransactionInitialTimeout,
                    httpHeaders: this.config.rpc.httpHeaders,
                    disableRetryOnRateLimit: this.config.rpc.disableRetryOnRateLimit
                });
                this.connections.set(`fallback-${index}`, connection);
            });
            if (this.config.rpc.wsEndpoint) {
                const wsConnection = new web3_js_1.Connection(this.config.rpc.wsEndpoint, {
                    commitment: this.config.rpc.commitment,
                    wsEndpoint: this.config.rpc.wsEndpoint
                });
                this.connections.set('websocket', wsConnection);
            }
            this.logger.info('Connections initialized successfully');
        }
        catch (error) {
            this.logger.error('Failed to initialize connections', { error });
            throw error;
        }
    }
    startConfigWatcher() {
        if (process.env.NODE_ENV === 'production') {
            this.watchInterval = setInterval(() => {
                this.checkConfigChanges();
            }, 5000);
        }
    }
    checkConfigChanges() {
        try {
            if (!(0, fs_1.existsSync)(this.configPath))
                return;
            const currentConfig = JSON.parse((0, fs_1.readFileSync)(this.configPath, 'utf-8'));
            const currentHash = (0, crypto_1.createHash)('sha256').update(JSON.stringify(currentConfig)).digest('hex');
            if (currentHash !== this.configHash) {
                this.logger.info('Configuration change detected, reloading...');
                this.reloadConfig();
            }
        }
        catch (error) {
            this.logger.error('Error checking config changes', { error });
        }
    }
    reloadConfig() {
        try {
            const newConfig = this.loadConfig();
            const oldConfig = this.config;
            this.config = newConfig;
            this.configHash = this.calculateConfigHash();
            if (oldConfig.rpc.primary !== newConfig.rpc.primary ||
                JSON.stringify(oldConfig.rpc.fallback) !== JSON.stringify(newConfig.rpc.fallback)) {
                this.initializeConnections();
            }
            this.emit('configReloaded', { old: oldConfig, new: newConfig });
            this.logger.info('Configuration reloaded successfully');
        }
        catch (error) {
            this.logger.error('Failed to reload configuration', { error });
            this.emit('configReloadError', error);
        }
    }
    getConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config)));
    }
    getRPCConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.rpc)));
    }
    getWalletConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.wallet)));
    }
    getTradingConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.trading)));
    }
    getStrategyConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.strategy)));
    }
    getMonitoringConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.monitoring)));
    }
    getSecurityConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.security)));
    }
    getNetworkConfig() {
        return Object.freeze(JSON.parse(JSON.stringify(this.config.network)));
    }
    getConnection(type = 'primary') {
        const connection = this.connections.get(type);
        if (!connection) {
            throw new Error(`Connection type ${type} not found`);
        }
        return connection;
    }
    async getHealthyConnection() {
        for (const [key, connection] of this.connections) {
            try {
                const slot = await connection.getSlot('finalized');
                if (slot) {
                    return connection;
                }
            }
            catch (error) {
                this.logger.warn(`Connection ${key} health check failed`, { error });
            }
        }
        this.logger.error('No healthy connections available');
        throw new Error('No healthy connections available');
    }
    getKeypair() {
        if (!this.keypair) {
            try {
                const privateKeyArray = JSON.parse(this.config.wallet.privateKey);
                this.keypair = web3_js_1.Keypair.fromSecretKey(new Uint8Array(privateKeyArray));
                const publicKey = this.keypair.publicKey.toBase58();
                if (publicKey !== this.config.wallet.publicKey) {
                    throw new Error('Public key mismatch');
                }
            }
            catch (error) {
                this.logger.error('Failed to load keypair', { error });
                throw error;
            }
        }
        return this.keypair;
    }
    updateConfig(updates) {
        try {
            const newConfig = { ...this.config, ...updates, lastUpdated: Date.now() };
            const { error, value } = CONFIG_SCHEMA.validate(newConfig, {
                abortEarly: false,
                stripUnknown: true
            });
            if (error) {
                throw new Error(`Invalid configuration update: ${error.message}`);
            }
            this.config = value;
            this.saveConfig(this.config);
            this.configHash = this.calculateConfigHash();
            this.emit('configUpdated', updates);
            this.logger.info('Configuration updated', { updates });
        }
        catch (error) {
            this.logger.error('Failed to update configuration', { error });
            throw error;
        }
    }
    updateTradingStatus(enabled) {
        this.updateConfig({
            trading: { ...this.config.trading, tradingEnabled: enabled }
        });
    }
    updateStrategyParameters(parameters) {
        this.updateConfig({
            strategy: { ...this.config.strategy, parameters }
        });
    }
    isTokenBlacklisted(token) {
        return this.config.trading.blacklistTokens.includes(token);
    }
    isTokenWhitelisted(token) {
        if (this.config.trading.whitelistTokens.length === 0) {
            return !this.isTokenBlacklisted(token);
        }
        return this.config.trading.whitelistTokens.includes(token);
    }
    addToBlacklist(token) {
        if (!this.isTokenBlacklisted(token)) {
            const blacklistTokens = [...this.config.trading.blacklistTokens, token];
            this.updateConfig({
                trading: { ...this.config.trading, blacklistTokens }
            });
        }
    }
    removeFromBlacklist(token) {
        const blacklistTokens = this.config.trading.blacklistTokens.filter(t => t !== token);
        this.updateConfig({
            trading: { ...this.config.trading, blacklistTokens }
        });
    }
    validateRiskParameters() {
        const { trading, strategy } = this.config;
        if (trading.maxSlippageBps > 1000 && strategy.riskLevel === 'conservative') {
            this.logger.warn('High slippage with conservative risk level');
            return false;
        }
        if (trading.maxPositionSize > trading.minPositionSize * 1000) {
            this.logger.warn('Position size range too wide');
            return false;
        }
        if (trading.stopLossPercent > trading.profitTargetPercent) {
            this.logger.warn('Stop loss greater than profit target');
            return false;
        }
        return true;
    }
    getPerformanceMetrics() {
        return {
            configVersion: this.config.version,
            lastUpdated: this.config.lastUpdated,
            connectionsActive: this.connections.size,
            tradingEnabled: this.config.trading.tradingEnabled,
            riskLevel: this.config.strategy.riskLevel,
            maxOpenPositions: this.config.trading.maxOpenPositions
        };
    }
    exportConfig() {
        const exportData = {
            ...this.config,
            wallet: {
                ...this.config.wallet,
                privateKey: '[REDACTED]'
            },
            security: {
                ...this.config.security,
                encryptionKey: '[REDACTED]'
            }
        };
        return JSON.stringify(exportData, null, 2);
    }
    importConfig(configData) {
        try {
            const importedConfig = JSON.parse(configData);
            if (importedConfig.wallet?.privateKey === '[REDACTED]') {
                importedConfig.wallet.privateKey = this.config.wallet.privateKey;
            }
            if (importedConfig.security?.encryptionKey === '[REDACTED]') {
                importedConfig.security.encryptionKey = this.config.security.encryptionKey;
            }
            const { error, value } = CONFIG_SCHEMA.validate(importedConfig, {
                abortEarly: false,
                stripUnknown: true
            });
            if (error) {
                throw new Error(`Invalid configuration import: ${error.message}`);
            }
            this.config = value;
            this.saveConfig(this.config);
            this.configHash = this.calculateConfigHash();
            this.initializeConnections();
            this.emit('configImported', this.config);
            this.logger.info('Configuration imported successfully');
        }
        catch (error) {
            this.logger.error('Failed to import configuration', { error });
            throw error;
        }
    }
    dispose() {
        if (this.watchInterval) {
            clearInterval(this.watchInterval);
        }
        for (const connection of this.connections.values()) {
            // connection.removeAllListeners(); // Not available on Connection
        }
        this.connections.clear();
        this.removeAllListeners();
        this.logger.info('ConfigManager disposed');
    }
}
exports.ConfigManager = ConfigManager;
// Export singleton instance getter
const getConfigManager = () => {
    return ConfigManager.getInstance();
};
exports.getConfigManager = getConfigManager;
// Export config validation function
const validateConfig = (config) => {
    const { error } = CONFIG_SCHEMA.validate(config, {
        abortEarly: false,
        stripUnknown: true
    });
    if (error) {
        return {
            valid: false,
            errors: error.details.map(detail => detail.message)
        };
    }
    return { valid: true };
};
exports.validateConfig = validateConfig;
// Export utility functions
const createConfigFromEnv = () => {
    return {
        rpc: {
            primary: process.env.RPC_PRIMARY || '',
            fallback: [
                process.env.RPC_FALLBACK_1 || '',
                process.env.RPC_FALLBACK_2 || ''
            ].filter(Boolean),
            commitment: process.env.RPC_COMMITMENT || 'confirmed',
            confirmTransactionInitialTimeout: parseInt(process.env.TX_TIMEOUT || '60000'),
            wsEndpoint: process.env.WS_ENDPOINT,
            disableRetryOnRateLimit: process.env.DISABLE_RETRY === 'true'
        },
        wallet: {
            privateKey: process.env.WALLET_PRIVATE_KEY || '',
            publicKey: process.env.WALLET_PUBLIC_KEY || '',
            maxConcurrentTransactions: parseInt(process.env.MAX_CONCURRENT_TX || '10'),
            priorityFeeMode: process.env.PRIORITY_FEE_MODE || 'dynamic',
            priorityFeeValue: parseInt(process.env.PRIORITY_FEE_VALUE || '1000'),
            computeUnitLimit: parseInt(process.env.COMPUTE_UNIT_LIMIT || '400000')
        },
        trading: {
            maxPositionSize: parseFloat(process.env.MAX_POSITION_SIZE || '100'),
            minPositionSize: parseFloat(process.env.MIN_POSITION_SIZE || '0.1'),
            maxSlippageBps: parseInt(process.env.MAX_SLIPPAGE_BPS || '300'),
            defaultSlippageBps: parseInt(process.env.DEFAULT_SLIPPAGE_BPS || '100'),
            maxGasPrice: parseFloat(process.env.MAX_GAS_PRICE || '0.01'),
            profitTargetPercent: parseFloat(process.env.PROFIT_TARGET_PERCENT || '2'),
            stopLossPercent: parseFloat(process.env.STOP_LOSS_PERCENT || '1'),
            trailingStopPercent: parseFloat(process.env.TRAILING_STOP_PERCENT || '0.5'),
            maxOpenPositions: parseInt(process.env.MAX_OPEN_POSITIONS || '5'),
            cooldownPeriodMs: parseInt(process.env.COOLDOWN_PERIOD_MS || '5000'),
            blacklistTokens: process.env.BLACKLIST_TOKENS?.split(',').filter(Boolean) || [],
            whitelistTokens: process.env.WHITELIST_TOKENS?.split(',').filter(Boolean) || [],
            tradingEnabled: process.env.TRADING_ENABLED !== 'false'
        },
        network: {
            chainId: process.env.CHAIN_ID || 'mainnet-beta',
            programIds: {},
            tokenListUrl: process.env.TOKEN_LIST_URL || 'https://token.jup.ag/all',
            priceOracleEndpoints: process.env.PRICE_ORACLE_ENDPOINTS?.split(',').filter(Boolean) || [],
            blockExplorerUrl: process.env.BLOCK_EXPLORER_URL || 'https://explorer.solana.com'
        }
    };
};
exports.createConfigFromEnv = createConfigFromEnv;
// Export default instance
exports.default = ConfigManager.getInstance();
//# sourceMappingURL=ConfigManager.js.map