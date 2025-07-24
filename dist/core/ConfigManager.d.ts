import { Connection, Keypair, Commitment } from '@solana/web3.js';
import { EventEmitter } from 'events';
export interface RPCConfig {
    primary: string;
    fallback: string[];
    commitment: Commitment;
    confirmTransactionInitialTimeout: number;
    httpHeaders?: Record<string, string>;
    wsEndpoint?: string;
    disableRetryOnRateLimit: boolean;
}
export interface WalletConfig {
    privateKey: string;
    publicKey: string;
    maxConcurrentTransactions: number;
    priorityFeeMode: 'dynamic' | 'fixed' | 'percentile';
    priorityFeeValue: number;
    computeUnitLimit: number;
}
export interface TradingConfig {
    maxPositionSize: number;
    minPositionSize: number;
    maxSlippageBps: number;
    defaultSlippageBps: number;
    maxGasPrice: number;
    profitTargetPercent: number;
    stopLossPercent: number;
    trailingStopPercent: number;
    maxOpenPositions: number;
    cooldownPeriodMs: number;
    blacklistTokens: string[];
    whitelistTokens: string[];
    tradingEnabled: boolean;
}
export interface StrategyConfig {
    type: 'arbitrage' | 'sniper' | 'sandwich' | 'liquidation' | 'market-making';
    parameters: Record<string, any>;
    riskLevel: 'conservative' | 'moderate' | 'aggressive';
    rebalanceIntervalMs: number;
    minProfitThreshold: number;
    emergencyExitEnabled: boolean;
}
export interface MonitoringConfig {
    metricsEnabled: boolean;
    metricsPort: number;
    alertWebhook?: string;
    performanceTrackingInterval: number;
    healthCheckInterval: number;
    logLevel: 'error' | 'warn' | 'info' | 'debug';
    logRetentionDays: number;
}
export interface SecurityConfig {
    encryptionEnabled: boolean;
    encryptionKey?: string;
    ipWhitelist: string[];
    maxRequestsPerMinute: number;
    autoLockAfterInactivity: number;
    requireMFA: boolean;
}
export interface NetworkConfig {
    chainId: 'mainnet-beta' | 'devnet' | 'testnet';
    programIds: Record<string, string>;
    tokenListUrl: string;
    priceOracleEndpoints: string[];
    blockExplorerUrl: string;
}
export interface Config {
    rpc: RPCConfig;
    wallet: WalletConfig;
    trading: TradingConfig;
    strategy: StrategyConfig;
    monitoring: MonitoringConfig;
    security: SecurityConfig;
    network: NetworkConfig;
    version: string;
    lastUpdated: number;
}
export declare class ConfigManager extends EventEmitter {
    private static instance;
    private config;
    private configPath;
    private logger;
    private configHash;
    private watchInterval?;
    private connections;
    private keypair?;
    private constructor();
    static getInstance(): ConfigManager;
    private initializeLogger;
    private loadConfig;
    private createDefaultConfig;
    private saveConfig;
    private calculateConfigHash;
    private initializeConnections;
    private startConfigWatcher;
    private checkConfigChanges;
    private reloadConfig;
    getConfig(): Readonly<Config>;
    getRPCConfig(): Readonly<RPCConfig>;
    getWalletConfig(): Readonly<WalletConfig>;
    getTradingConfig(): Readonly<TradingConfig>;
    getStrategyConfig(): Readonly<StrategyConfig>;
    getMonitoringConfig(): Readonly<MonitoringConfig>;
    getSecurityConfig(): Readonly<SecurityConfig>;
    getNetworkConfig(): Readonly<NetworkConfig>;
    getConnection(type?: 'primary' | 'websocket' | string): Connection;
    getHealthyConnection(): Promise<Connection>;
    getKeypair(): Keypair;
    updateConfig(updates: Partial<Config>): void;
    updateTradingStatus(enabled: boolean): void;
    updateStrategyParameters(parameters: Record<string, any>): void;
    isTokenBlacklisted(token: string): boolean;
    isTokenWhitelisted(token: string): boolean;
    addToBlacklist(token: string): void;
    removeFromBlacklist(token: string): void;
    validateRiskParameters(): boolean;
    getPerformanceMetrics(): Record<string, any>;
    exportConfig(): string;
    importConfig(configData: string): void;
    dispose(): void;
}
export declare const getConfigManager: () => ConfigManager;
export declare const validateConfig: (config: any) => {
    valid: boolean;
    errors?: string[];
};
export type ConfigChangeHandler = (oldConfig: Config, newConfig: Config) => void;
export type ConfigErrorHandler = (error: Error) => void;
export declare const createConfigFromEnv: () => Partial<Config>;
declare const _default: ConfigManager;
export default _default;
//# sourceMappingURL=ConfigManager.d.ts.map