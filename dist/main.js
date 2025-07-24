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
Object.defineProperty(exports, "__esModule", { value: true });
const web3_js_1 = require("@solana/web3.js");
const FlashLoanRouter_1 = require("./flashloan-advanced/FlashLoanRouter");
const fs = __importStar(require("fs"));
async function main() {
    console.log('🚀 Starting Solana Flash Loan Bot...');
    // Configuration
    const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com';
    const WALLET_PATH = process.env.WALLET_PATH || './wallet.json';
    try {
        // Load wallet
        let wallet;
        if (fs.existsSync(WALLET_PATH)) {
            const walletData = JSON.parse(fs.readFileSync(WALLET_PATH, 'utf-8'));
            wallet = web3_js_1.Keypair.fromSecretKey(new Uint8Array(walletData));
            console.log('✅ Wallet loaded:', wallet.publicKey.toString());
        }
        else {
            console.log('⚠️  No wallet found, generating new one...');
            wallet = web3_js_1.Keypair.generate();
            fs.writeFileSync(WALLET_PATH, JSON.stringify(Array.from(wallet.secretKey)));
            console.log('✅ New wallet created:', wallet.publicKey.toString());
            console.log('⚠️  Please fund this wallet before running the bot!');
            return;
        }
        // Connect to Solana
        const connection = new web3_js_1.Connection(RPC_ENDPOINT, 'confirmed');
        console.log('✅ Connected to Solana:', RPC_ENDPOINT);
        // Check balance
        const balance = await connection.getBalance(wallet.publicKey);
        console.log('💰 Wallet balance:', balance / 1000000000, 'SOL');
        if (balance < 50000000) { // 0.05 SOL minimum
            console.error('❌ Insufficient balance. Need at least 0.05 SOL');
            return;
        }
        // Initialize router
        const router = new FlashLoanRouter_1.FlashLoanRouter(connection, wallet);
        // Check provider availability
        const stats = await router.getProviderStats();
        console.log('\n📊 Provider Status:');
        stats.forEach((stat, provider) => {
            console.log(`  ${provider}: ${stat.available ? '✅ Available' : '❌ ' + stat.error}`);
        });
        console.log('\n🔄 Bot is running... Press Ctrl+C to stop');
        console.log('⚠️  This is a test implementation. Use at your own risk!');
        // Main loop would go here
        // For now, just keep the process alive
        process.on('SIGINT', () => {
            console.log('\n👋 Shutting down gracefully...');
            process.exit(0);
        });
    }
    catch (error) {
        console.error('❌ Fatal error:', error);
        process.exit(1);
    }
}
main().catch(console.error);
//# sourceMappingURL=main.js.map